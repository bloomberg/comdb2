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

/**
 * We keep an in-memory structure of the pending blocksql transactions
 * We avoid repeating blocksql requests and wait for the first transaction to
 * commit before sending back an answer
 */

#include <stdio.h>
#include <pthread.h>
#include <plhash.h>
#include <poll.h>
#include "comdb2.h"
#include "osqlblkseq.h"
#include "logmsg.h"

int gbl_block_blkseq_poll = 10; /* 10 msec */

static hash_t *hiqs = NULL;
static pthread_rwlock_t hlock = PTHREAD_RWLOCK_INITIALIZER;

/*
 * Init this module
 *
 */
int osql_blkseq_init(void)
{
    int rc = 0;

    Pthread_rwlock_wrlock(&hlock);

    hiqs = hash_init_o(offsetof(struct ireq, seq), sizeof(fstblkseq_t));
    if (!hiqs) {
        logmsg(LOGMSG_ERROR, 
                "UNABLE TO init a hash? ignoring blocksql blockseq optimization\n");
        rc = -1;
    }

    Pthread_rwlock_unlock(&hlock);

    return rc;
}

/**
 * Main function
 * - check to see if the seq exists
 * - if this is a replay, return OSQL_BLOCKSEQ_REPLAY
 * - if this is NOT a replay, insert the seq and return OSQL_BLOCKSEQ_FIRST
 *
 */
int osql_blkseq_register(struct ireq *iq)
{
    struct ireq *iq_src = NULL;
    int rc = 0;

    Pthread_rwlock_wrlock(&hlock);

    if (!hiqs) {
        rc = OSQL_BLOCKSEQ_INV;
        goto done;
    }

    iq_src = hash_find(hiqs, (const void *)&iq->seq);

    if (!iq_src) {
        /* first time */
        hash_add(hiqs, iq);
        rc = OSQL_BLOCKSEQ_FIRST;
        goto done;
    } else {
        /* wait for */
        while (1) {
            /* losing the write lock first run */
            Pthread_rwlock_unlock(&hlock);
            poll(NULL, 0, gbl_block_blkseq_poll);

            /* rdlock will suffice */
            Pthread_rwlock_rdlock(&hlock);

            iq_src = hash_find_readonly(hiqs, (const void *)&iq->seq);
            if (!iq_src) {
                /* done waiting */
                rc = OSQL_BLOCKSEQ_REPLAY;
                goto done;
            }
            /* keep searching */
        }
    }

done:
    Pthread_rwlock_unlock(&hlock);
    return rc;
}

/**
 * Remove a blkseq from memory hash so that the next blocksql
 * repeated transactions can proceed ahead
 *
 */
int osql_blkseq_unregister(struct ireq *iq)
{
    Pthread_rwlock_wrlock(&hlock);

    if (hiqs) /* Fix a deadlock */
        hash_del(hiqs, iq);

    Pthread_rwlock_unlock(&hlock);
    return 0;
}
