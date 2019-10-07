/*
   Copyright 2015, 2018, Bloomberg Finance L.P.

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

#ifndef INCLUDED_INDICES_H
#define INCLUDED_INDICES_H

int check_for_upsert(struct ireq *iq, void *trans, struct schema *ondisktagsc,
                     blob_buffer_t *blobs, size_t maxblobs, int *opfailcode,
                     int *ixfailnum, int *retrc, const char *ondisktag,
                     void *od_dta, size_t od_len, unsigned long long ins_keys,
                     int rec_flags);

int add_record_indices(struct ireq *iq, void *trans, blob_buffer_t *blobs,
                       size_t maxblobs, int *opfailcode, int *ixfailnum,
                       int *rrn, unsigned long long *genid,
                       unsigned long long vgenid, unsigned long long ins_keys,
                       int opcode, int blkpos, void *od_dta, size_t od_len,
                       const char *ondisktag, struct schema *ondisktagsc,
                       int flags, bool reorder);

int upd_record_indices(struct ireq *iq, void *trans, int *opfailcode,
                       int *ixfailnum, int rrn, unsigned long long *newgenid,
                       unsigned long long ins_keys, int opcode, int blkpos,
                       void *od_dta, size_t od_len, void *old_dta,
                       unsigned long long del_keys, int flags,
                       blob_buffer_t *add_idx_blobs,
                       blob_buffer_t *del_idx_blobs, int same_genid_with_upd,
                       unsigned long long vgenid, int *deferredAdd);

int del_record_indices(struct ireq *iq, void *trans, int *opfailcode,
                       int *ixfailnum, int rrn, unsigned long long genid,
                       void *od_dta, unsigned long long del_keys, int flags,
                       blob_buffer_t *del_idx_blobs, const char *ondisktag);

int upd_new_record_indices(
    struct ireq *iq, void *trans, unsigned long long newgenid,
    unsigned long long ins_keys, const void *new_dta, const void *old_dta,
    int use_new_tag, void *sc_old, void *sc_new, int nd_len,
    unsigned long long del_keys, blob_buffer_t *add_idx_blobs,
    blob_buffer_t *del_idx_blobs, unsigned long long oldgenid, int verify_retry,
    int deferredAdd);

int del_new_record_indices(struct ireq *iq, void *trans,
                           unsigned long long ngenid, const void *old_dta,
                           int use_new_tag, void *sc_old,
                           unsigned long long del_keys,
                           blob_buffer_t *del_idx_blobs, int verify_retry);

#endif
