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

#ifndef INCLUDE_SC_CALLBACKS_H
#define INCLUDE_SC_CALLBACKS_H

#include <bdb_schemachange.h>

int is_genid_right_of_stripe_pointer(bdb_state_type *bdb_state,
                                     unsigned long long genid,
                                     unsigned long long stripe_ptr);

int live_sc_post_delete_int(struct ireq *iq, void *trans,
                            unsigned long long genid, const void *old_dta,
                            unsigned long long del_keys,
                            blob_buffer_t *oldblobs);

int live_sc_post_add_int(struct ireq *iq, void *trans, unsigned long long genid,
                         const uint8_t *od_dta, unsigned long long ins_keys,
                         blob_buffer_t *blobs, size_t maxblobs, int origflags,
                         int *rrn);

int live_sc_post_update_int(struct ireq *iq, void *trans,
                            unsigned long long oldgenid, const void *old_dta,
                            unsigned long long newgenid, const void *new_dta,
                            unsigned long long ins_keys,
                            unsigned long long del_keys, int od_len,
                            int *updCols, blob_buffer_t *blobs, int deferredAdd,
                            blob_buffer_t *oldblobs, blob_buffer_t *newblobs);

int live_sc_post_update_delayed_key_adds_int(struct ireq *iq, void *trans,
                                             unsigned long long newgenid,
                                             const void *od_dta,
                                             unsigned long long ins_keys,
                                             int od_len);

int scdone_callback(bdb_state_type *bdb_state, const char table[],
                    scdone_t type);

int schema_change_abort_callback(void);

void sc_del_unused_files(struct dbtable *);
void sc_del_unused_files_tran(struct dbtable *, tran_type *);
void sc_del_unused_files_check_progress(void);

void getMachineAndTimeFromFstSeed(const char **mach, time_t *timet);

#endif
