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

#ifndef INCLUDED_LOCALREP_H
#define INCLUDED_LOCALREP_H

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "block_internal.h"

int local_replicant_log_add(struct ireq *iq, void *trans, void *od_dta,
                            blob_buffer_t *blobs, int *opfailcode);
int local_replicant_log_delete_for_update(struct ireq *iq, void *trans, int rrn,
                                          unsigned long long genid,
                                          int *opfailcode);
int local_replicant_log_delete(struct ireq *iq, void *trans, void *od_dta,
                               int *opfailcode);
int local_replicant_log_add_for_update(struct ireq *iq, void *trans, int rrn,
                                       unsigned long long new_genid,
                                       int *opfailcode);
int local_replicant_genid_by_seqno_blkpos(struct ireq *iq, long long seqno, 
                                          int blkpos, unsigned long long *outgenid);
int add_local_commit_entry(struct ireq *iq, void *trans, long long seqno, long long seed, int nops);

#endif
