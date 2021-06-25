/*
   Copyright 2021 Bloomberg Finance L.P.

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

#ifndef INCLUDED_OSQLBUNDLED_H
#define INCLUDED_OSQLBUNDLED_H

#include "comdb2.h"
#include "block_internal.h"

extern int gbl_osql_max_bundled_bytes;
/* Initialize bundling on top of an osql target. */
void init_bplog_bundled(osql_target_t *target);
/* Extract snap-info from a bundle. */
void osql_extract_snap_info_from_bundle(osql_sess_t *sess, void *buf, int len, int is_uuid);
/* Process a bundle. */
int osql_process_bundled(struct ireq *iq, unsigned long long rqid, uuid_t uuid, void *trans, char *msg, int msglen,
                         int *flags, int **updCols, blob_buffer_t blobs[MAXBLOBS], int step, struct block_err *err,
                         int *receivedrows);
/* Copy rqid to a bundle. rqid is needed when wrapping up a bundle. */
void copy_rqid(osql_target_t *target, unsigned long long rqid, uuid_t uuid);
#endif
