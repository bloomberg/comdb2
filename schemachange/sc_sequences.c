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

#include "schemachange.h"
#include "logmsg.h"

/**
 * Checks sequence parameters to ensure that the configured sequence is valid.
 * Returns 0 if valid and non-zero if not valid.
 */
int validate_sequence(long long min_val, long long max_val, long long increment,
                      int cycle, long long start_val, long long restart_val,
                      long long chunk_size)
{
    if (min_val > max_val) {
        logmsg(LOGMSG_ERROR,
               "Minvalue of %lld and Maxvalue of %lld are invalid\n", min_val,
               max_val);
        return 1;
    }

    if (chunk_size < 1) {
        logmsg(LOGMSG_ERROR, "Chunk size of %lld is invalid\n", chunk_size);
        return 1;
    }

    if (increment == 0) {
        logmsg(LOGMSG_ERROR, "Increment of %lld is invalid\n", chunk_size);
        return 1;
    }

    if (increment > chunk_size) {
        logmsg(LOGMSG_ERROR, "Increment of %lld is invalid\n", increment);
        return 1;
    }

    if (start_val > max_val || start_val < min_val) {
        logmsg(LOGMSG_ERROR, "Start Value of %lld is invalid\n", start_val);
        return 1;
    }

    if (restart_val > max_val || restart_val < min_val) {
        logmsg(LOGMSG_ERROR, "Restart Value of %lld is invalid\n", restart_val);
        return 1;
    }

    return 0;
}

/**
 * Adds sequence to llmeta and memory
 */
int do_add_sequence_int(struct schema_change_type *s, tran_type *trans)
{
    char *name = s->table;
    long long min_val = s->seq_min_val;
    long long max_val = s->seq_max_val;
    long long increment = s->seq_increment;
    int cycle = s->seq_cycle;
    long long start_val = s->seq_start_val;
    long long chunk_size = s->seq_chunk_size;
    char flags = 0;
    SBUF2 *sb = s->sb;
    int rc = 0;
    int bdberr;

    // Check that name is valid
    if (strlen(name) > MAXTABLELEN - 1) {
        logmsg(LOGMSG_ERROR,
               "sequence name too long. Must be less than %d characters\n",
               MAXTABLELEN - 1);
        return 1;
    }

    // Check for duplicate name
    if (!(getsequencebyname(name) == NULL)) {
        logmsg(LOGMSG_ERROR, "sequence with name \"%s\" already exists\n",
               name);
        return 1;
    }

    // Check that there aren't too many sequences
    if (thedb->num_sequences >= MAX_NUM_SEQUENCES) {
        logmsg(LOGMSG_ERROR, "Max number of sequences created. Unable to "
                             "create new sequence.\n");
        return 1;
    }

    // Validate sequence attributes
    if (validate_sequence(min_val, max_val, increment, cycle, start_val,
                          start_val, chunk_size)) {
        // Invalid sequence configuration
        logmsg(LOGMSG_ERROR,
               "Invalid parameters provided. Sequence cannot be created.\n");
        return 1;
    }

    // Add sequence to llmeta
    rc = bdb_llmeta_add_sequence(trans, name, min_val, max_val, increment,
                                 cycle, start_val, start_val, chunk_size, flags,
                                 &bdberr);

    if (rc) {
        logmsg(LOGMSG_ERROR, "can't create new sequence \"%s\"\n", name);
        return -1;
    }

    // Make space in memory
    thedb->sequences = realloc(thedb->sequences,
                               (thedb->num_sequences + 1) * sizeof(sequence_t));
    if (thedb->sequences == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for sequences list\n");
        return 1;
    }

    // Create new sequence in memory
    sequence_t *seq =
        new_sequence(name, min_val, max_val, start_val, increment, cycle,
                     start_val, chunk_size, flags, 0, start_val);

    if (seq == NULL) {
        logmsg(LOGMSG_ERROR, "can't create sequence \"%s\"\n", name);
        return -1;
    }

    thedb->sequences[thedb->num_sequences] = seq;
    thedb->num_sequences++;

    /* log for replicants to do the same */
    rc = bdb_llog_sequences_tran(thedb->bdb_env, name, llmeta_sequence_add, trans, &bdberr);
    if (rc) {
        sbuf2printf(sb, "Failed to broadcast sequence add\n");
        logmsg(LOGMSG_ERROR, "Failed to broadcast sequence ad\n");
    }

    return rc;
}

/**
 * Drops sequence from llmeta and memory
 */
int do_drop_sequence_int(struct schema_change_type *s, tran_type *trans)
{
    int rc;
    int bdberr;
    int i;
    char *name = s->table;
    SBUF2 *sb = s->sb;

    if (thedb->num_sequences == 0) {
        // No Sequences Defined
        logmsg(LOGMSG_ERROR, "No sequences defined\n");
        return 1;
    }

    for (i = 0; i < thedb->num_sequences; i++) {
        if (strcasecmp(thedb->sequences[i]->name, name) == 0) {
            // TODO: add checks for usage in tables
            // Remove sequence from dbenv
            thedb->num_sequences--;

            if (thedb->num_sequences > 0) {
                thedb->sequences[i] = thedb->sequences[thedb->num_sequences];
            }

            thedb->sequences[thedb->num_sequences] = NULL;

            // Remove llmeta record
            rc = bdb_llmeta_drop_sequence(trans, name, &bdberr);

            if (rc) return rc;

            /* log for replicants to do the same */
            rc = bdb_llog_sequences_tran(thedb->bdb_env, name, llmeta_sequence_drop, trans, &bdberr);
            if (rc) {
                sbuf2printf(sb, "Failed to broadcast sequence drop\n");
                logmsg(LOGMSG_ERROR, "Failed to broadcast sequence drop\n");
                return rc;
            }

            return 0;
        }
    }

    logmsg(LOGMSG_ERROR, "sequence with name \"%s\" does not exists\n", name);
    return 1;
}

/**
 * Alters the sequence definition in llmeta and updates in memory
 * representations on the master and replicants
 */
int do_alter_sequence_int(struct schema_change_type *s, tran_type *trans)
{
    char *name = s->table;
    long long min_val_in = s->seq_min_val;
    long long max_val_in = s->seq_max_val;
    long long increment_in = s->seq_increment;
    int cycle_in = s->seq_cycle;
    long long start_val_in = s->seq_start_val;
    long long restart_val_in = s->seq_restart_val;
    long long chunk_size_in = s->seq_chunk_size;
    int modified = s->seq_modified;
    SBUF2 *sb = s->sb;
    int rc = 0;
    int bdberr;

    if (thedb->num_sequences == 0) {
        // No Sequences Defined
        logmsg(LOGMSG_ERROR, "No sequences defined\n");
        return 1;
    }

    sequence_t *seq = getsequencebyname(name);
    if (seq == NULL) {
        // Failed to find sequence with specified name
        logmsg(LOGMSG_ERROR, "Sequence %s cannot be found\n", name);
        return -1;
    }

    // Get lock for in memory object
    pthread_mutex_lock(&seq->seq_lk);

    // Min Val
    long long min_val = seq->min_val;
    if (modified & SEQ_MIN_VAL) {
        min_val = min_val_in;
    }

    // Max Val
    long long max_val = seq->max_val;
    if (modified & SEQ_MAX_VAL) {
        max_val = max_val_in;
    }

    // Increment
    long long increment = seq->increment;
    if (modified & SEQ_INC) {
        increment = increment_in;
    }

    // Cycle
    bool cycle = seq->cycle;
    if (modified & SEQ_CYCLE) {
        cycle = cycle_in;
    }

    // Start Val
    long long start_val = seq->start_val;
    if (modified & SEQ_START_VAL) {
        start_val = start_val_in;
    }

    // Restart Val
    long long restart_val = seq->next_start_val;
    if (modified & SEQ_RESTART_TO_START_VAL) {
        restart_val = seq->start_val;

        // Unraise exhausted flag
        seq->flags &= ~SEQUENCE_EXHAUSTED;
    } else if (modified & SEQ_RESTART_VAL) {
        restart_val = restart_val_in;

        // Unraise exhausted flag
        seq->flags &= ~SEQUENCE_EXHAUSTED;
    }

    // Chunk Size
    long long chunk_size = seq->chunk_size;
    if (modified & SEQ_CHUNK_SIZE) {
        chunk_size = chunk_size_in;
    }

    // Validate sequence attributes
    if (validate_sequence(min_val, max_val, increment, cycle, start_val,
                          restart_val, chunk_size)) {
        // Invalid sequence configuration
        logmsg(LOGMSG_ERROR,
               "Invalid parameters provided. Sequence cannot be altered.\n");
        pthread_mutex_unlock(&seq->seq_lk);
        return 1;
    }

    // Write change to llmeta
    rc = bdb_llmeta_alter_sequence(trans, name, min_val, max_val, increment,
                                   cycle, start_val, restart_val, chunk_size,
                                   seq->flags, &bdberr);
    if (rc) {
        pthread_mutex_unlock(&seq->seq_lk);
        return rc;
    }

    // Update environment
    seq->min_val = min_val;
    seq->max_val = max_val;
    seq->increment = increment;
    seq->cycle = cycle;
    seq->start_val = start_val;
    seq->next_start_val = restart_val;
    seq->chunk_size = chunk_size;
    seq->remaining_vals = 0;

    pthread_mutex_unlock(&seq->seq_lk);

    /* log for replicants to do the same */
    rc = bdb_llog_sequences_tran(thedb->bdb_env, name, llmeta_sequence_alter, trans, &bdberr);
    if (rc) {
        sbuf2printf(sb, "Failed to broadcast sequence alter\n");
        logmsg(LOGMSG_ERROR, "Failed to broadcast sequence alter\n");
    }

    return rc;
}
