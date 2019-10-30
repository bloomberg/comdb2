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

#include <build/db.h>
#include <bdb/bdb_int.h>

extern int gbl_logical_live_sc;

struct common_members {
    int64_t ndeadlocks;
    int64_t nlockwaits;
    uint32_t lkcountercheck_lasttime; // used for checking lockwaints
    uint32_t thrcount;           // number of threads currently available
    uint32_t maxthreads;         // maximum number of SC threads allowed
    int is_decrease_thrds;       // is feature on to backoff and decrease threads
    uint32_t total_lasttime;     // last time we computed total stats
};

/* for passing state data to schema change threads/functions */
struct convert_record_data {
    pthread_t tid;
    int isThread;
    struct schema_change_type *s;
    void *dta_buf;
    void *old_dta_buf;
    void *unpack_dta_buf;
    void *unpack_old_dta_buf;
    void *blb_buf;
    void *old_blb_buf;
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
    /* all the data objects point to the same single cmembers object */
    struct common_members *cmembers;
    unsigned int write_count; // saved write counter to this tbl
    DB_LSN start_lsn;
    hash_t *blob_hash;
    struct odh odh;
    struct odh oldodh;
    DB_LSN cv_wait_lsn; /* for logical_livesc, wait for redo thread to catup at
                           this LSN if we get constraint violations when
                           converting the records */
    unsigned long long cv_genid; /* the genid of the record that we get
                                    constraint violation on */
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

void *live_sc_logical_redo_thd(struct convert_record_data *data);
#endif
