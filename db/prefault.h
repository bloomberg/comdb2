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

#ifndef __BDB_PFLT_H__
#define __BDB_PFLT_H__

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

enum pfrq_type {
    PFRQ_OLDDATA = 1, /* given a table, genid : fault the dta record */
    PFRQ_OLDKEY = 5,  /* given a table, key   : fault the ix record  */
    PFRQ_NEWKEY = 6,  /* given a table, key   : fault the ix record  */

    PFRQ_OLDDATA_OLDKEYS = 3, /* given a table, genid : 1) fault the dta record.
                                                  2) then form all keys, and
                                                     enque PRFQ_OLDKEY for each
                        */
    PFRQ_OLDDATA_OLDKEYS_NEWKEYS = 4, /* given a table,genid :
                                      1) fault the dta record.
                                      2) then form all keys from found record
                                         and enque PRFQ_OLDKEY for each
                                      3) form new record based on found
                                         record + input record and
                                      4) form all keys from new record and
                                         enque PRFQ_NEWKEY for each
                                */

    PFRQ_EXITTHD = 7
};

typedef struct {
    int num_add_record;
    int num_del_record;
    int num_upd_record;

    int num_prfq_data;
    int num_prfq_key;
    int num_prfq_data_keys;
    int num_prfq_data_keys_newkeys;

    int num_prfq_data_broadcast;
    int num_prfq_key_broadcast;
    int num_prfq_data_keys_broadcast;
    int num_prfq_data_keys_newkeys_broadcast;

    int num_prfq_data_received;
    int num_prfq_key_received;
    int num_prfq_data_keys_received;
    int num_prfq_data_keys_newkeys_received;

    int num_prfq_data_keys_newkeys_no_olddta;

    int num_ioq_full;
    int num_nohelpers;

    int skipped;
    int skipped_seq;
    int processed;

    int aborts;

} prefault_stats_type;

typedef struct prefaultiopool {
    int guard;
    pthread_mutex_t mutex;
    pthread_cond_t cond;

    int numthreads;
    int maxq;
    pthread_t threads[128]; /* XXX yeah, make this better */
    queue_type *ioq;
} prefaultiopool_type;

typedef struct pfrq {
    short type;
    struct dbtable *db;
    unsigned short ix;
    unsigned long long genid;
    int index;
    unsigned char key[512];
    void *record;
    unsigned short len; /* if its a key, the len of the key.  if its a dta rec,
                           the len of the record */
    char *tag;          /* i have no idea how long this is allowed to be, so its
                           dynamic (mainly due to dynamic tags!) */
    int taglen;
    int broadcast;
    int dolocal;
    int flush;

    unsigned int opnum;
    struct ireq *iq;
    int helper_thread;
    unsigned int seqnum;
} pfrq_t;

enum { PREFAULT_TOBLOCK = 1, PREFAULT_READAHEAD = 2 };

typedef struct {
    int type;

    pthread_t working_for; /* the tid of the thread we are working for,
                              set to gbl_inalid_tid if idle */

    /* for toblock prefaulting */
    pthread_t tid;
    pthread_cond_t cond;
    pthread_mutex_t mutex;

    int helper_thread; /* what helper thread do we work for (-1 if none) */
    void *iq;

    /* for readahead prefaulting */
    struct dbtable *db;
    short ixnum;
    short keylen;
    short numreadahead;
    unsigned char key[512];
    int abort;

    unsigned char *pfk_bitmap;

    void *blkstate;

    unsigned int seqnum;
} prefault_helper_thread_type;

typedef struct {
    int numthreads;
    pthread_mutex_t mutex;
    prefault_helper_thread_type threads[64];
} prefault_helper_type;

typedef struct {
    struct dbenv *dbenv;
    int instance;
} prefault_helper_thread_arg_type;

typedef struct {
    pthread_t tid;
    pthread_cond_t cond;
    pthread_mutex_t mutex;
    void *blkstate;
    void *iq;
} readahead_thread_type;

typedef struct {
    int numthreads;
    pthread_mutex_t mutex;
    readahead_thread_type threads[64];
} readaheadprefault_type;

unsigned int enque_pfault_ll(struct dbenv *dbenv, pfrq_t *qdata);

int enque_pfault_olddata(struct dbtable *db, unsigned long long genid, int opnum,
                         int helper_thread, unsigned int seqnum, int broadcast,
                         int dolocal, int flush);

int enque_pfault_oldkey(struct dbtable *db, void *key, int keylen, int ixnum,
                        int opnum, int helper_thread, unsigned int seqnum,
                        int broadcast, int dolocal, int flush);

int enque_pfault_newkey(struct dbtable *db, void *key, int keylen, int ixnum,
                        int opnum, int helper_thread, unsigned int seqnum,
                        int broadcast, int dolocal, int flush);

int enque_pfault_olddata_oldkeys(struct dbtable *db, unsigned long long genid,
                                 int opnum, int helper_thread,
                                 unsigned int seqnum, int broadcast,
                                 int dolocal, int flush);

int enque_pfault_olddata_oldkeys_newkeys(struct dbtable *db,
                                         unsigned long long genid, char *tag,
                                         int taglen, void *record, int reclen,
                                         int opnum, int helper_thread,
                                         unsigned int seqnum, int broadcast,
                                         int dolocal, int flush);

int enque_pfault_exit(struct dbenv *dbenv);

/* return 1 if prefault enabled, else 0 */
int prefault_check_enabled(void);

int create_prefault_helper_threads(struct dbenv *dbenv, int nthreads);
int start_prefault_io_threads(struct dbenv *dbenv, int numthreads, int maxq);

int prefault_toblock(struct ireq *iq, void *blkstate, int helper_thread,
                     unsigned int seqnum, int *abort);
int prefault_readahead(struct dbtable *db, int ixnum, unsigned char *key, int keylen,
                       int num);

/* call this to initiate a readahead */
int readaheadpf(struct ireq *iq, struct dbtable *db, int ixnum, unsigned char *key,
                int keylen, int num);

void prefault_stats(struct dbenv *dbenv);

void prefault_free(pfrq_t *pflt);

void prefault_kill_bits(struct ireq *iq, int ixnum, int type);

#endif
