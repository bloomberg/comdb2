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
#include <bdb_int.h>
#include <bdb/locks.h>
#include <sql.h>

extern struct dbenv *thedb;

int gbl_sequence_replicant_distribution = 0;
int request_sequence_range_from_master(bdb_state_type *bdb_state,
                                       const char *name_in,
                                       sequence_range_t *val);
int verify_master_leases(bdb_state_type *bdb_state, const char *func,
                         uint32_t line);

/**
 * Returns the next value for a specified sequence object. If the sequence name
 * cannot be found, -1 is returned. If the next value is greater than the max or
 * less than min value, for ascending or descending sequences respectively, and
 * cycle is not enabled, <error>.
 *
 *  @param name char * Name of the sequence
 *  @param val long long * Reference to output location
 */
static int seq_next_val_int(tran_type *tran, char *name, long long *val,
                            bdb_state_type *bdb_state)
{
    sequence_t *seq = getsequencebyname(name);
    sequence_range_t *temp;
    sequence_range_t *node;
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
    if (seq->range_head == NULL) {
        // Create new node range
        node = malloc(sizeof(sequence_range_t));

        if (node == NULL) {
            logmsg(LOGMSG_ERROR,
                   "can't allocate memory for new chunk for sequence \"%s\"\n",
                   name);
            goto done;
        }

        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
            // I am master, generate and return value
            if (bdb_state->attr->master_lease &&
                !verify_master_leases(bdb_state, __func__, __LINE__)) {
                logmsg(LOGMSG_ERROR,
                       "%s line %d failed verifying master leases\n", __func__,
                       __LINE__);
                rc = -1;
                free(node);
                goto done;
            }

            // No remaining values, allocate new chunk into range
            rc = bdb_llmeta_get_sequence_chunk(
                tran, name, seq->min_val, seq->max_val, seq->increment,
                seq->cycle, seq->chunk_size, &seq->flags, seq->start_val,
                &seq->next_start_val, node, &bdberr);

            if (rc) {
                logmsg(LOGMSG_ERROR,
                       "can't retrive new chunk for sequence \"%s\"\n", name);
                free(node);
                goto done;
            }

        } else {
            // I am replicant (rep distribution must be on)
            pthread_mutex_unlock(&seq->seq_lk);

            // Call to master for new range
            rc = request_sequence_range_from_master(bdb_state, name, node);

            if (rc) {
                logmsg(LOGMSG_ERROR,
                       "can't retrive new chunk for sequence \"%s\"\n", name);
                free(node);
                goto done;
            }

            // Get lock for in-memory object
            pthread_mutex_lock(&seq->seq_lk);
        }

        // Insert node into ranges linked list
        insert_sequence_range(seq, node);
    }

    // Dispense next_val
    *val = seq->range_head->current;

    // Calculate next value to dispense
    // Check for integer overflow
    if (overflow_ll_add(seq->range_head->current, seq->increment)) {
        goto next_range;
    }

    // Apply increment, no overflow can occur
    seq->range_head->current += seq->increment;

    // Check for out of range conditions
    if ((seq->increment > 0) &&
            (seq->range_head->current > seq->range_head->max_val) ||
        (seq->increment < 0) &&
            (seq->range_head->current < seq->range_head->min_val)) {
        goto next_range;
    }

done:
    pthread_mutex_unlock(&seq->seq_lk);
    return rc;

next_range:
    // Current head range is empty, move to next range
    temp = seq->range_head;
    seq->range_head = temp->next;

    if (temp->next) {
        free(temp);
    }

    goto done;
}

int seq_next_val(tran_type *tran, char *name, long long *val,
                 bdb_state_type *bdb_state)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->sqlclntstate;
    int rc, lid = bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran);
    DB_LOCK dblk;

    /* Get curtran & use lockid from there.  Can this be a deadlock victim? */
    if (rc = bdb_lock_seq_read_fromlid(bdb_state, name, (void *)&dblk, lid)) {
        return SQLITE_DEADLOCK;
    }

    rc = seq_next_val_int(tran, name, val, bdb_state);
    bdb_release_lock(bdb_state, (void *)&dblk);
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

        // Remove any allocated values to force chunk reallocation
        pthread_mutex_lock(&seq->seq_lk);
        remove_sequence_ranges(seq);
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

        // Remove any allocated values
        remove_sequence_ranges(seq);

        pthread_mutex_unlock(&seq->seq_lk);
    }
}

int insert_sequence_range(sequence_t *seq, sequence_range_t *node)
{
    if (seq->range_head) {
        sequence_range_t *cur_node = seq->range_head;
        while (cur_node->next) {
            cur_node = cur_node->next;
        }

        cur_node->next = node;
        node->next = NULL;
    } else {
        seq->range_head = node;
        node->next = NULL;
    }

    return 0;
}

int generate_replicant_sequence_range(char *name, sequence_range_t *range)
{
    int rc = 0;
    int bdberr = 0;
    sequence_t *seq = getsequencebyname(name);

    if (seq == NULL) {
        // Failed to find sequence with specified name
        logmsg(LOGMSG_ERROR, "Sequence %s cannot be found\n", name);
        return -1;
    }

    // Get lock for in-memory object
    pthread_mutex_lock(&seq->seq_lk);

    rc = bdb_llmeta_get_sequence_chunk(
        NULL, name, seq->min_val, seq->max_val, seq->increment, seq->cycle,
        seq->chunk_size, &seq->flags, seq->start_val, &seq->next_start_val,
        range, &bdberr);

    // Get lock for in-memory object
    pthread_mutex_unlock(&seq->seq_lk);

    if (rc) {
        logmsg(LOGMSG_ERROR, "can't retrive new chunk for sequence \"%s\"\n",
               name);
        return rc;
    }

    return 0;
}
