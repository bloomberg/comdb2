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

#include "sequences.h"
#include <int_overflow.h>

extern struct dbenv *thedb;

/**
 *  Returns the next value for a specified sequence object. If the sequence name
 *  cannot be found, -1 is returned. If the next value is greater than the max
 *  or less than min value, for ascending or descending sequences respectively,
 *  and cycle is not enabled, <error>.
 *
 *  @param name char * Name of the sequence
 *  @param val long long * Reference to output location
 */
int seq_next_val(char *name, long long *val)
{
    sequence_t *seq = getsequencebyname(name);
    int rc = 0;
    int bdberr = 0;

    if (seq == NULL) {
        // Failed to find sequence with specified name
        // TODO: error out another way
        logmsg(LOGMSG_ERROR, "Sequence %s cannot be found\n", name);
        return -1;
    }

    // Get lock for in-memory object
    pthread_mutex_lock(&seq->seq_lk);

    // Check for remaining values. 
    // seq->next_val is only valid if remaining values > 0
    if (seq->remaining_vals == 0) {
        // No remaining values, allocate new chunk
        rc = bdb_llmeta_get_sequence_chunk(
            NULL, name, seq->min_val, seq->max_val, seq->increment, seq->cycle,
            seq->chunk_size, &seq->flags, &seq->remaining_vals, seq->start_val,
            &seq->next_val, &seq->next_start_val, &bdberr);

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "can't retrive new chunk for sequence \"%s\"\n", name);
            goto done;
        }

        if (seq->remaining_vals == 0) {
            logmsg(LOGMSG_ERROR, "No more sequence values for '%s'\n", name);
            rc = -1;
            goto done;
        }
    }

    // Dispense next_val
    *val = seq->next_val;
    seq->remaining_vals--;

    // Calculate next value to dispense
    long long next_val = seq->next_val;

    // Check for integer overflow
    if (overflow_ll_add(seq->next_val, seq->increment)) {
        if (seq->cycle) {
            if (seq->increment > 0)
                seq->next_val = seq->min_val;
            else
                seq->next_val = seq->max_val;
        } else {
            // No more sequence values to dispense. Value of next_val is now
            // undefined behaviour and unreliable.
            seq->remaining_vals = 0;
        }

        goto done;
    }

    // Apply increment, no overflow can occur
    seq->next_val += seq->increment;

    // Check for cycle conditions
    if ((seq->increment > 0) && (seq->next_val > seq->max_val)) {
        if (seq->cycle) {
            seq->next_val = seq->min_val;
        } else {
            // No more sequence values to dispense. Value of next_val is now
            // undefined behaviour and unreliable.
            seq->remaining_vals = 0;
        }
    } else if ((seq->increment < 0) && (seq->next_val < seq->min_val)) {
        if (seq->cycle) {
            seq->next_val = seq->max_val;
        } else {
            // No more sequence values to dispense. Value of next_val is now
            // undefined behaviour and unreliable.
            seq->remaining_vals = 0;
        }
    }

done:
    pthread_mutex_unlock(&seq->seq_lk);
    return rc;
}
