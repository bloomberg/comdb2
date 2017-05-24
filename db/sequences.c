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

extern struct dbenv *thedb;
/*
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

   //TODO: Protect crit section
   *val = seq->next_val;

   // Increment Value
   long long inc = seq->increment;
   long long max = seq->max_val;
   long long min = seq->min_val;

   seq->prev_val = seq->next_val;
   seq->next_val += inc; // TODO: check for overflow

   if ( ((inc >= 0) && (seq->next_val > max)) || ((inc < 0) && (seq->next_val < min)) ) {
      if (seq->cycle && inc >= 0) {
         seq->next_val = min;
      } else if (seq->cycle && inc < 0) {
         seq->next_val = max;
      } else {
         // TODO: Error End of sequence
         return -1;
      }
   }

   return 1;
}

/*
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

    return 1;
}

/*
 *  Create sequence objects in memory from definitions in llmeta
 */
int llmeta_load_sequences ( struct dbenv *dbenv ) {
   //TODO: Move to bdb/llmeta.c

   // get names (and count) from llmeta
   // Create sequence_t and add to dbenv from llmeta defns

}

/*
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

/*
 * REMOVE: Test Function for adding a sequence directly to memory
 */
int add_sequence (char* name, long long min_val, long long max_val, 
   long long increment, bool cycle, 
   long long start_val, long long chunk_size)
{
    // init dbenv
    if (thedb->num_sequences <= 0) {
        thedb->num_sequences = 0;
        thedb->sequences = realloc(thedb->sequences,
                          4 * sizeof(sequence_t));
    }
    
    if (thedb->sequences == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for sequence list\n");
        return -1;
    }

    if (!(get_sequence(name) == NULL)){
        logmsg(LOGMSG_ERROR, "sequence with name \"%s\" already exists\n", name);
        return -1;
    }

    sequence_t * new_seq = malloc(sizeof(sequence_t));
    if (new_seq == NULL) {
        logmsg(LOGMSG_ERROR, "can't allocate memory for new sequence\n");
        return -1;
    }

    new_seq->name = name;
    new_seq->min_val = min_val;
    new_seq->max_val = max_val;
    new_seq->increment = increment;
    new_seq->cycle = cycle;
    new_seq->prev_val = 0;
    new_seq->next_val = 0;
    new_seq->chunk_size = chunk_size;
   
    thedb->sequences[thedb->num_sequences++] = new_seq;

    return 1;
}
