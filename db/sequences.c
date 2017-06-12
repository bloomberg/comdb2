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
 *  cannot be found, -1 is returned. If the next value is greater than the max or
 *  less than min value, for ascending or decesending sequences respectively, and
 *  cycle is not enabled, <error>.
 *
 *  @param name char * Name of the sequence
 *  @param val long long * Reference to output location
 */
int seq_next_val (char *name, long long *val) {
    sequence_t *seq = get_sequence(name);

    if ( seq == NULL ) {
       // Failed to find sequence with specified name
       // TODO: error out another way
       return -1;
    }

    //TODO: Protect crit section with lock
    *val = seq->next_val;

    // Increment Value
    long long inc = seq->increment;
    long long max = seq->max_val;
    long long min = seq->min_val;
    long long next_val = seq->next_val;

    int dir = inc < 0 ? -1 : 1; // Direction of the sequence: 1 - Increasing, -1 - Decreasing

    // TODO: Check for last value allocated, deal with cycles
    // if ( (dir*(seq->next_val + inc) > dir*(seq->last_avail_val) ) {
        
    // }

    seq->prev_val = seq->next_val;

    // Check for integer overflow
    if (!check_overflow_ll_add(next_val, inc))
        return 0;

    next_val += inc;

    if ( ((inc >= 0) && (next_val > max)) || ((inc < 0) && (next_val < min)) ) {
       if (seq->cycle && inc >= 0) {
          next_val = min;
       } else if (seq->cycle && inc < 0) {
          next_val = max;
       } else {
          // TODO: Error End of sequence
          return -1;
       }
    }

    seq->next_val = next_val;
    return 0;
}


/**
 *  TODO: Rename to something else? Last value?
 *
 *  Returns the previous value that was dispensed for a specified sequence object.
 *  If the sequence name cannot be found, -1 is returned. If a previous value does
 *  not exist, <error>.
 *
 *  @param name char * Name of the sequence
 *  @param val long long * Reference to output location
 */
int seq_prev_val (char *name, long long *val) {
    sequence_t *seq = get_sequence(name);

    if ( seq == NULL ) {
        // Failed to find sequence with specified name
        // TODO: error out another way
        return -1;
    }

    if ( seq->prev_val == seq->next_val ) {
        // Previous Value doesn't exist
        // TODO: error out another way
        return -1;
    }

    *val = seq->prev_val;

    return 0;
}


/**
 *  Helper to return a pointer to a sequence by name.
 *
 *  @param name char * Name of the sequence
 */
sequence_t *get_sequence(char *name) {
    int i;

    if (thedb->num_sequences == 0) {
        // No Sequences Defined
        return NULL;
    }

    for(i = 0; i < thedb->num_sequences; i++) {
        if ( strcmp(thedb->sequences[i]->name, name) == 0) {
            return thedb->sequences[i];
        }
    }

    return NULL; //TODO: FIX ERROR
}


/**
 * TODO: MOVE SOMEWHERE ELSE
 * 
 * Adds sequence to llmeta and memory
 */
int add_sequence (char* name, long long min_val, long long max_val,
    long long increment, bool cycle,
    long long start_val, long long chunk_size)
{
    // Check that name is valid
    if (strlen(name) > MAXTABLELEN - 1){
        logmsg(LOGMSG_ERROR, "sequence name too long. Must be less than %d characters\n", MAXTABLELEN-1);
        return 1;
    }

    // Check for duplicate name
    if (!(get_sequence(name) == NULL)){
        logmsg(LOGMSG_ERROR, "sequence with name \"%s\" already exists\n", name);
        return 1;
    }

    // Check that there aren't too many sequences
    if (thedb->num_sequences >= MAX_NUM_SEQUENCES){
        logmsg(LOGMSG_ERROR, "Max number of sequences created. Unable to create new sequence.");
        return 1;
    }

    // Make space in memory
    thedb->sequences = realloc(thedb->sequences, (thedb->num_sequences + 1) * sizeof(sequence_t));

    if (thedb->sequences == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for sequences list\n");
        return 1;
    }

    // Create entry in memory
    thedb->sequences[thedb->num_sequences] = new_sequence(name, min_val, max_val, increment, cycle, start_val, chunk_size, start_val + (chunk_size*increment) );

    if(thedb->sequences[thedb->num_sequences]==NULL){
        logmsg(LOGMSG_ERROR, "Failed to create sequence \"%s\"\n", name);
        return 1;
    }

    // Create llmeta record
    int rc;
    int bdberr;
    rc = bdb_llmeta_add_sequence(NULL, name, min_val, max_val, increment, cycle, start_val, chunk_size, &bdberr);

    if (rc)
        return rc;

    thedb->num_sequences++;

    return 0;
}

/**
 * TODO: MOVE SOMEWHERE ELSE
 * 
 * Drops sequence from llmeta and memory
 */
int drop_sequence (char *name) {
    int rc;
    int bdberr;
    int i;

    if (thedb->num_sequences == 0) {
        // No Sequences Defined
        logmsg(LOGMSG_ERROR, "No sequences defined\n");
        return 1;
    }

    for(i = 0; i < thedb->num_sequences; i++) {
        if ( strcmp(thedb->sequences[i]->name, name) == 0) {
            // TOOD: add checks for usage in tables

            // TODO: Crit section?
            // Remove sequence from dbenv

            thedb->num_sequences--;
            
            if (thedb->num_sequences > 0){
                thedb->sequences[i] = thedb->sequences[thedb->num_sequences];
            }
            
            thedb->sequences[thedb->num_sequences] = NULL;

            // Remove llmeta record
            rc = bdb_llmeta_drop_sequence(NULL, name, &bdberr);

            if (rc)
                return rc;

            return 0;
        }
    }

    logmsg(LOGMSG_ERROR, "sequence with name \"%s\" does not exists\n", name);
    return 1;
}
