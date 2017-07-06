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
 * TODO: Make Transactional
 *
 * Adds sequence to llmeta and memory
 */
int do_add_sequence_int(char *name, long long min_val, long long max_val,
                 long long increment, int cycle, long long start_val,
                 long long chunk_size)
{
    char flags = 0;

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
        logmsg(
            LOGMSG_ERROR,
            "Max number of sequences created. Unable to create new sequence.");
        return 1;
    }

    // Add sequence to llmeta
    int rc, bdberr;

    rc = bdb_llmeta_add_sequence(NULL, name, min_val, max_val, increment, cycle,
                                 start_val, chunk_size, flags, &bdberr);

    if (rc) {
        logmsg(LOGMSG_ERROR, "can't create new sequence \"%s\"\n", name);
        return -1;
    }

    // Allocate value chunk
    long long next_start_val = start_val;
    long long remaining_vals;
    sequence_t *seq;

    rc = bdb_llmeta_get_sequence_chunk(
        NULL, name, min_val, max_val, increment, cycle, chunk_size, &flags,
        &remaining_vals, &next_start_val, &bdberr);

    if (rc) {
        logmsg(LOGMSG_ERROR, "can't retrive new chunk for sequence \"%s\"\n",
               name);
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
    seq = new_sequence(name, min_val, max_val, increment, cycle, start_val,
                       chunk_size, flags, remaining_vals, next_start_val);

    if (seq == NULL) {
        logmsg(LOGMSG_ERROR, "can't create sequence \"%s\"\n", name);
        return -1;
    }

    thedb->sequences[thedb->num_sequences] = seq;
    thedb->num_sequences++;

    return 0;
}

/**
 * TODO: Make Transactional
 * Drops sequence from llmeta and memory
 */
int do_drop_sequence_int(char *name)
{
    int rc;
    int bdberr;
    int i;

    if (thedb->num_sequences == 0) {
        // No Sequences Defined
        logmsg(LOGMSG_ERROR, "No sequences defined\n");
        return 1;
    }

    for (i = 0; i < thedb->num_sequences; i++) {
        if (strcasecmp(thedb->sequences[i]->name, name) == 0) {
            // TOOD: add checks for usage in tables

            // TODO: Crit section?

            // Remove sequence from dbenv
            thedb->num_sequences--;

            if (thedb->num_sequences > 0) {
                thedb->sequences[i] = thedb->sequences[thedb->num_sequences];
            }

            thedb->sequences[thedb->num_sequences] = NULL;

            // Remove llmeta record
            rc = bdb_llmeta_drop_sequence(NULL, name, &bdberr);

            if (rc) return rc;

            return 0;
        }
    }

    logmsg(LOGMSG_ERROR, "sequence with name \"%s\" does not exists\n", name);
    return 1;
}
