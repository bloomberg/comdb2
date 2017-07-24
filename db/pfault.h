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

#define MAXBRDBUFSZ (1024 * 1024)

enum pfrq_type {
    PFRQ_KEY = 1,       /* prefetch single ondisk-format key */
    PFRQ_DATAADD = 2,   /* prefetch for adds-get new genid and prefetch block
                           based on that value */
    PFRQ_DATAGENID = 3, /* prefetch data block based on existing genid */
    PFRQ_DATAFRMKY =
        4, /* prefetch data block by genid + form all associated keys w/
              that record and prefetch them too */
    PFRQ_DEFPRIMKY = 5, /* find record based on primary key in 'default' format.
                           then form all keys and prefetch them too */
    PFRQ_DEFDTAFRMKY =
        6, /* find record based on primary key but form primary key from
              provided default data record */
    PFRQ_EXITTHD = 7
};

typedef struct pfbb {
    u_char *buf;
    int offset;
    int length;
    int cnt;
    /*  int      index_ref_count;
    pthread_mutex_t bufmtx;*/
} pfbb_t;

typedef struct pfdpack {
    int packlen;
    int dbnamelen;
    char data[1];
} pfdpack_t;

typedef struct pfrq {
    unsigned long long genid;
    int type;
    int index;
    int length;
    struct dbtable *db;
    pfbb_t *bbuf;
    char data[1];
} pfrq_t;

int pflt_queue_upd_record(struct ireq *iq, void *primkey, int rrn,
                          unsigned long long vgenid, const char *tagdescr,
                          size_t taglen, void *record, void *vrecord,
                          size_t reclen, unsigned char fldnullmap[32],
                          int flags, pfbb_t *buf);

int pflt_queue_add_record(struct ireq *iq, const char *tagdescr, size_t taglen,
                          void *record, size_t reclen,
                          unsigned char fldnullmap[32], int flags, pfbb_t *buf);

int pflt_queue_del_record(struct ireq *iq, void *primkey, void *defdta, int rrn,
                          unsigned long long genid, int flags, pfbb_t *buf);

int enque_pfault_index(struct dbtable *db, void *key, int keylen, int ixnum,
                       pfbb_t *bbuf);
int enque_pfault_dtastripe_exit(struct dbtable *db, pfbb_t *bbuf);
int enque_pfault_dtastripe_add(struct dbtable *db, pfbb_t *bbuf);
int enque_pfault_dtastripe_genid_formkey(struct dbtable *db,
                                         unsigned long long genid,
                                         pfbb_t *bbuf);
int enque_pfault_dtastripe_genid(struct dbtable *db, unsigned long long genid,
                                 pfbb_t *bbuf);
int enque_pfault_dtastripe_defprimkey_formkey(struct dbtable *db, void *pkey,
                                              int pkeylen, pfbb_t *bbuf);
int enque_pfault_dtastripe_defprimdata_formkey(struct dbtable *db, void *ddata,
                                               int ddlen, pfbb_t *bbuf);

int stop_dealloc_all_pfault(struct dbtable *db);
int start_pfault_dtastripe(struct dbtable *db, int npflt);
int start_pfault_dta(struct dbtable *db, int npflt);
int start_pfault_idx(struct dbtable *db, int npflt);
int start_pfault_all(struct dbenv *dbenv, struct dbtable *db);

int add_to_brdbuf(pfbb_t *bbuf, int type, void *data, int dtalen);
int flush_brdbuf(pfbb_t *bbuf);
int requeue_brdbuf(u_char *bufdata, int buflen);
int last_thread_flush_brdbuf(pfbb_t *bbuf);
#endif
