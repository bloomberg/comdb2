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

#ifndef _OSQLBLKSEQ_H_
#define _OSQLBLKSEQ_H_

/* get your ret codes here */
enum {
    OSQL_BLOCKSEQ_INV = 0,
    OSQL_BLOCKSEQ_FIRST = 1,
    OSQL_BLOCKSEQ_REPLAY = 2
};

extern int gbl_block_blockseq_poll;

/**
 * Init this module
 *
 */
int osql_blkseq_init(void);

/**
 * Main function to do same as osql_blkseq_register snap_info was passed
 * - check to see if the cnonce exists
 * - if this is a replay, return OSQL_BLOCKSEQ_REPLAY
 * - if this is NOT a replay, insert the seq and return OSQL_BLOCKSEQ_FIRST
 *
 */
int osql_blkseq_register_cnonce(struct ireq *iq);

/**
 * Main function
 * - check to see if the seq exists
 * - if this is a replay, return OSQL_BLOCKSEQ_REPLAY
 * - if this is NOT a replay, insert the seq and return OSQL_BLOCKSEQ_FIRST
 *
 */
int osql_blkseq_register(struct ireq *iq);

/**
 * Remove a blkseq from memory hash so that the next blocksql
 * repeated transactions can proceed ahead
 *
 */
int osql_blkseq_unregister(struct ireq *iq);

#endif
