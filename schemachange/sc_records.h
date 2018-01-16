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

#ifndef INCLUDE_SC_RECORDS_H
#define INCLUDE_SC_RECORDS_H

struct common_members {
    int64_t ndeadlocks;
    int64_t nlockwaits;
    int lkcountercheck_lasttime; /* used for checking lockwaints */
    int thrcount;                // number of threads currently available
    int maxthreads;              // maximum number of SC threads allowed
    int is_decrease_thrds; // is feature on to backoff and decrease threads
};

/* for passing state data to schema change threads/functions */
struct convert_record_data {
    pthread_t tid;
    int isThread;
    struct schema_change_type *s;
    void *dta_buf;
    tran_type *trans;
    enum convert_scan_mode scanmode;
    int live, lastrrn, lasttime, outrc;
    unsigned int totnretries;
    struct ireq iq;
    struct dbtable *from, *to;
    unsigned long long *sc_genids;
    int stripe;
    struct dtadump *dmp;
    char *lastkey, *curkey;
    char key1[MAXKEYLEN], key2[MAXKEYLEN];
    unsigned long long lastgenid;
    blob_status_t blb, blbcopy;
    struct dbrecord *rec;
    blob_buffer_t wrblb[MAXBLOBS];
    blob_buffer_t freeblb[MAXBLOBS];
    int blobix[MAXBLOBS], toblobs2fromblobs[MAXBLOBS];
    unsigned n_genids_changed;
    long long nrecs, prev_nrecs, nrecskip;
    int num_records_per_trans;
    int num_retry_errors;
    int *tagmap; // mapping of fields from -> to
    struct common_members *cmembers;
    unsigned int write_count; // saved write counter to this tbl
};

int convert_all_records(struct dbtable *from, struct dbtable *to,
                        unsigned long long *sc_genids,
                        struct schema_change_type *s);

int upgrade_all_records(struct dbtable *db, unsigned long long *sc_genids,
                        struct schema_change_type *s);

void *convert_records_thd(struct convert_record_data *data);

void convert_record_data_cleanup(struct convert_record_data *data);

int init_sc_genids(struct dbtable *db, struct schema_change_type *s);

void live_sc_enter_exclusive_all(bdb_state_type *, tran_type *);
#endif
