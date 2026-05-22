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

#ifndef _OSQL_SRS_H_
#define _OSQL_SRS_H_

#include "comdb2.h"
#include "sql.h"

enum {
    /* No retry in progress. Initial state, or set after the transaction
     * committed, received a non-retryable error, or exhausted retries. */
    OSQL_RETRY_NONE = 0,
    /* A retry is in progress. Do not send results back to the client yet. */
    OSQL_RETRY_DO = 1,
    /* The last retry is in progress. We must send results back to the client
     * regardless of the outcome. */
    OSQL_RETRY_LAST = 3,
    /* A retry failed with a non-retryable error. Do not retry further and send
     * the error back to the client. */
    OSQL_RETRY_HALT = 4
};

/**
 * Create a history of sql for this transaction
 * that will allow replay in the case of verify errors
 */
int srs_tran_create(struct sqlclntstate *clnt);

/**
 * Destroy the sql transaction history
 *
 */
int srs_tran_destroy(struct sqlclntstate *clnt);

/**
 * Add a new query to the transaction
 *
 */
int srs_tran_add_query(struct sqlclntstate *clnt);

/**
 * delete the query to the transaction
 *
 */
int srs_tran_del_last_query(struct sqlclntstate *clnt);

/**
 * Empty the context of the transaction
 *
 */
int srs_tran_empty(struct sqlclntstate *clnt);

/**
 * Prepare and dispatch the replay
 */
int srs_tran_replay_prepare(struct sqlclntstate *clnt);

/**
 * Replay transaction once using the current history
 *
 */
int srs_tran_replay(struct sqlclntstate *clnt);

void srs_tran_print_history(struct sqlclntstate *clnt, int indent);
#endif
