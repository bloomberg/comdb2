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
static hash_t *hiqs_cnonce = NULL;
static pthread_mutex_t hmtx = PTHREAD_MUTEX_INITIALIZER;

unsigned int cnonce_hashfunc(const void *key, int len)
{
    snap_uid_t *o = (snap_uid_t *)key;
    return hash_default_fixedwidth((const unsigned char *)o->key, o->keylen);
}

int cnonce_hashcmpfunc(const void *key1, const void *key2, int len)
{
    snap_uid_t *o1, *o2;
    o1 = (snap_uid_t *)key1;
    o2 = (snap_uid_t *)key2;

    int minlen = o1->keylen < o2->keylen ? o1->keylen : o2->keylen;
    int cmp = memcmp(o1->key, o2->key, minlen);
    if (cmp)
        return cmp;
    if (o1->keylen == o2->keylen)
        return 0;
    if (o1->keylen > o2->keylen)
        return 1;
    return -1;
}

int osql_blkseq_register_cnonce(struct ireq *iq)
{
    void *iq_src = NULL;
    int rc = 0;

    assert(hiqs_cnonce != NULL);

    Pthread_mutex_lock(&hmtx);
    iq_src = hash_find(hiqs_cnonce, &iq->snap_info);
    if (!iq_src) { /* not there, we add it */
        hash_add(hiqs_cnonce, &iq->snap_info);
        rc = OSQL_BLOCKSEQ_FIRST;
    }
    Pthread_mutex_unlock(&hmtx);
    if (!iq_src) {
        logmsg(LOGMSG_DEBUG, "Added to blkseq %*s\n", iq->snap_info.keylen - 3,
               iq->snap_info.key);
    }

    /* rc == 0 means we need to wait for it to go away */
    while (rc == 0) {
        logmsg(LOGMSG_DEBUG, "Already in blkseq %*s, stalling...\n",
               iq->snap_info.keylen - 3, iq->snap_info.key);
        poll(NULL, 0, gbl_block_blkseq_poll);

        Pthread_mutex_lock(&hmtx);
        iq_src = hash_find_readonly(hiqs_cnonce, &iq->snap_info);
        Pthread_mutex_unlock(&hmtx);

        if (!iq_src) {
            /* done waiting */
            rc = OSQL_BLOCKSEQ_REPLAY;
        }
        /* keep searching */
    }

    return rc;
}

/* call with hmtx acquired */
static inline int osql_blkseq_unregister_cnonce(struct ireq *iq)
{
    assert(hiqs_cnonce != NULL);
    return hash_del(hiqs_cnonce, &iq->snap_info);
}

/*
 * Init this module
 *
 */
int osql_blkseq_init(void)
{
    int rc = 0;

    Pthread_mutex_lock(&hmtx);

    hiqs = hash_init_o(offsetof(struct ireq, seq), sizeof(fstblkseq_t));
    if (!hiqs) {
        logmsg(LOGMSG_FATAL, "UNABLE TO init hash\n");
        abort();
    }

    hiqs_cnonce = hash_init_user(cnonce_hashfunc, cnonce_hashcmpfunc, 0, 0);
    if (!hiqs) {
        logmsg(LOGMSG_FATAL, "UNABLE TO init cnonce hash\n");
        abort();
    }

    Pthread_mutex_unlock(&hmtx);

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

    assert(hiqs != NULL);

    Pthread_mutex_lock(&hmtx);
    iq_src = hash_find(hiqs, (const void *)&iq->seq);
    if (!iq_src) { /* not there, we add it */
        hash_add(hiqs, iq);
        rc = OSQL_BLOCKSEQ_FIRST;
    }
    Pthread_mutex_unlock(&hmtx);

    /* rc == 0 means we need to wait for it to go away */
    while (rc == 0) {
        poll(NULL, 0, gbl_block_blkseq_poll);

        Pthread_mutex_lock(&hmtx);
        iq_src = hash_find_readonly(hiqs, (const void *)&iq->seq);
        Pthread_mutex_unlock(&hmtx);

        if (!iq_src) {
            /* done waiting */
            rc = OSQL_BLOCKSEQ_REPLAY;
        }
        /* keep searching */
    }

    return rc;
}

/**
 * Remove a blkseq from memory hash so that the next blocksql
 * repeated transactions can proceed ahead
 *
 */
int osql_blkseq_unregister(struct ireq *iq)
{
    /* Fast return if have_blkseq is false.
       It not only saves quite a few instructions,
       but also avoids a race condition with osql_open() */
    if (!iq->have_blkseq)
        return 0;

    assert(hiqs != NULL);
    int rc = 0;

    Pthread_mutex_lock(&hmtx);

    hash_del(hiqs, iq);
    rc = osql_blkseq_unregister_cnonce(iq);

    Pthread_mutex_unlock(&hmtx);
    if (iq->have_snap_info)
        logmsg(LOGMSG_DEBUG, "Removed from blkseq %*s, rc=%d\n",
               iq->snap_info.keylen - 3, iq->snap_info.key, rc);
    return 0;
}
