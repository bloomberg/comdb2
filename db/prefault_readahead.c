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

#include <alloca.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>

#include <plbitlib.h>

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "translistener.h"
#include "block_internal.h"
#include "prefault.h"

#define MAXGENIDS 256

int prefault_readahead(struct dbtable *db, int ixnum, unsigned char *key, int keylen,
                       int num)
{
    unsigned long long genids[MAXGENIDS];
    int rc;
    int num_genids_gotten;
    int i;
    int num_gotten;
    struct ireq iq;

    init_fake_ireq(thedb, &iq);
    iq.usedb = db;

    if (num > MAXGENIDS)
        num = MAXGENIDS;

    /* get the next NUM genids from ix ixnum and enqueue faults for those
       dta records */
    rc = get_next_genids(&iq, ixnum, key, keylen, genids, num, &num_gotten);

    if (rc == 0) {
        for (i = 0; i < num_gotten; i++) {
            enque_pfault_olddata(db, genids[i], 0, -1, 0, 0, 1, 0);
        }
    }

    return 0;
}
