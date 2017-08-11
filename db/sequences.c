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
int seq_next_val(tran_type *tran, char *name, long long *val)
{
    sequence_t *seq = getsequencebyname(name);
    int rc = 0;
    int bdberr = 0;

    if (seq == NULL) {
        // Failed to find sequence with specified name
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
            tran, name, seq->min_val, seq->max_val, seq->increment, seq->cycle,
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

int sequences_master_change()
{
    int i;
    sequence_t *seq;

    /*should be changed to a hash table*/
    for (i = 0; i < thedb->num_sequences; i++) {
        seq = thedb->sequences[i];

        if (seq == NULL) {
            continue;
        }

        // Clear remaining_vals to force chunk reallocation
        pthread_mutex_lock(&seq->seq_lk);
        seq->remaining_vals = 0;
        pthread_mutex_unlock(&seq->seq_lk);
    }
}

int sequences_master_upgrade()
{
    int i, rc, bdberr;
    sequence_t *seq;

    /*should be changed to a hash table*/
    for (i = 0; i < thedb->num_sequences; i++) {
        seq = thedb->sequences[i];

        if (seq == NULL) {
            continue;
        }

        // Reload sequence descriptions from llmeta (for next_start_val)
        pthread_mutex_lock(&seq->seq_lk);

        rc = bdb_llmeta_get_sequence(
            NULL, seq->name, &seq->min_val, &seq->max_val, &seq->increment,
            &seq->cycle, &seq->start_val, &seq->next_start_val,
            &seq->chunk_size, &seq->flags, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't get information for sequence \"%s\"\n",
                   seq->name);
            return -1;
        }

        seq->remaining_vals = 0;

        pthread_mutex_unlock(&seq->seq_lk);
    }
}
