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

#ifndef INCLUDED_VERIFY_H
#define INCLUDED_VERIFY_H

typedef enum {
    VERIFY_SERIAL,
    VERIFY_PARALLEL,
    VERIFY_DATA,
    VERIFY_INDICES,
    VERIFY_BLOBS
} verify_mode_t;

/* pass as target_ix to look at every index of the table */
#define VERIFY_ALL_IXNUM (-1)

typedef struct {
    verify_mode_t mode;
    int progress_report_seconds;

    /* Patch the blob-length headers of a data record to match the blobs that
     * are actually there.  This is what the appsock's fix flag has always
     * meant; it does not touch indexes. */
    int attempt_fix;

    /* Add the index entry for records the data scan finds with no entry in an
     * index.  Repairs are additive: a record is only ever given a key it should
     * already have had, nothing is deleted.  Master only. */
    int fix_missing_keys;

    /* Restrict every index scan to this one index, VERIFY_ALL_IXNUM for all of
     * them.  Note this does not make the data scan read fewer records - it only
     * drops the per-index lookups done for each one. */
    int target_ix;
} verify_opts_t;

struct dbtable;
void purge_by_genid(struct dbtable *, unsigned long long *genid);
void dump_record_by_rrn_genid(struct dbtable *, int rrn, unsigned long long genid);

typedef int(verify_peer_check_func)(void *arg);
typedef int(verify_response_func)(char *response, void *arg);
int verify_table(const char *table, int progress_report_seconds,
                 int attempt_fix, verify_mode_t, verify_peer_check_func *,
                 verify_response_func *, void *arg);
int verify_table_opts(const char *table, const verify_opts_t *, verify_peer_check_func *, verify_response_func *,
                      void *arg);

#endif
