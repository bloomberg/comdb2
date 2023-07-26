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

/*#define PREFAULT_TRACE*/
/*#define PREFAULT_NOOP*/

#define PREFAULT_SKIP
#define PREFAULT_SKIP_SEQ

#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>

#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "block_internal.h"
#include "comdb2.h"
#include "util.h"
#include "prefault.h"
#include <assert.h>

#include <memory_sync.h>

#include <plbitlib.h>
#include <logmsg.h>

int gbl_prefault_verbose = 1;

extern pthread_key_t lockmgr_key;

static void *prefault_io_thread(void *arg);

/* initialize the prefault io pool.  pass set the number of io threads,
 and the queue depth */
int start_prefault_io_threads(struct dbenv *dbenv, int numthreads, int maxq)
{
    int i = 0;
    static int started = 0;
    pthread_attr_t attr;

    bzero(&(dbenv->prefault_stats), sizeof(prefault_stats_type));

    dbenv->prefaultiopool.numthreads = 0;

    if (numthreads == 0)
        return 0;
    if (maxq == 0)
        return 0;
    if (started != 0)
        return 0;

    Pthread_attr_init(&attr);

    Pthread_attr_setstacksize(&attr, 512 * 1024);

    Pthread_mutex_init(&dbenv->prefaultiopool.mutex, NULL);
    dbenv->prefaultiopool.guard = 0xabababab;
    logmsg(LOGMSG_DEBUG, "&(dbenv->prefaultiopool.guard) = %p\n",
           &(dbenv->prefaultiopool.guard));

    logmsg(LOGMSG_DEBUG, "prefault cond initialized\n");
    Pthread_cond_init(&dbenv->prefaultiopool.cond, NULL);

    dbenv->prefaultiopool.maxq = maxq;
    dbenv->prefaultiopool.ioq = queue_new();
    if (dbenv->prefaultiopool.ioq == NULL) {
        logmsg(LOGMSG_FATAL, "couldnt create prefault io queue\n");
        exit(1);
    }

    for (i = 0; i < numthreads; i++) {
        Pthread_create(&(dbenv->prefaultiopool.threads[i]), &attr,
                            prefault_io_thread, (void *)dbenv);
        dbenv->prefaultiopool.numthreads++;
    }
    started = 1;

    Pthread_attr_destroy(&attr);

    return 0;
}

int broadcast_prefault(struct dbenv *dbenv, pfrq_t *qdata);

unsigned int enque_pfault_ll(struct dbenv *dbenv, pfrq_t *qdata)
{
    int rc;

#ifdef PREFAULT_NOOP
    return 1;
#endif

    /* dont enqueue if we dont have any io threads */
    if (dbenv->prefaultiopool.numthreads == 0)
        return 1;

    Pthread_mutex_lock(&(dbenv->prefaultiopool.mutex));
    /*fprintf(stderr, "about to add item, q now=%d\n",
       queue_count(dbenv->prefaultiopool.ioq));*/

    if (queue_count(dbenv->prefaultiopool.ioq) >= dbenv->prefaultiopool.maxq) {
        /* queue over the allowed size..ignore the request! */
        Pthread_mutex_unlock(&(dbenv->prefaultiopool.mutex));

        dbenv->prefault_stats.num_ioq_full++;

        /* our queue is full... but still broadcast it anyway... */
        if (qdata->broadcast) {
            broadcast_prefault(dbenv, qdata);
        }

        /* it didnt add, we need to let the level above free it */
        return 1;
    }

    rc = queue_add(dbenv->prefaultiopool.ioq, qdata);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "could not add data to queue!\n");
        Pthread_mutex_unlock(&(dbenv->prefaultiopool.mutex));
    }

    Pthread_cond_signal(&(dbenv->prefaultiopool.cond));
    Pthread_mutex_unlock(&(dbenv->prefaultiopool.mutex));

    /*fprintf(stderr, "(%d) queued idx %d\n",(void *)pthread_self(), ixnum);*/
    return 0;
}

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_pfault_oldkey(struct dbtable *db, void *key, int keylen, int ixnum,
                        int opnum, int helper_thread, unsigned int seqnum,
                        int broadcast, int dolocal, int flush)
{
    pfrq_t *qdata = NULL;
    int rc;

    /* XXX this should be changed to a pool */
    qdata = malloc(sizeof(pfrq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc in io thread\n");
        exit(1);
    }

    qdata->type = PFRQ_OLDKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    qdata->db = db;
    qdata->genid = -1;
    if ((keylen > 0) && (keylen < MAXKEYLEN))
        memcpy(qdata->key, key, keylen);
    qdata->record = NULL;
    qdata->tag = NULL;
    qdata->broadcast = broadcast;
    qdata->dolocal = dolocal;
    qdata->flush = flush;

    qdata->opnum = opnum;
    qdata->helper_thread = helper_thread;
    qdata->seqnum = seqnum;

    /*fprintf(stderr, "enqueued PFRQ_DATA_KEY\n");*/

    rc = enque_pfault_ll(db->dbenv, qdata);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_pfault_newkey(struct dbtable *db, void *key, int keylen, int ixnum,
                        int opnum, int helper_thread, unsigned int seqnum,
                        int broadcast, int dolocal, int flush)
{
    pfrq_t *qdata = NULL;
    int rc;

    /* XXX this should be changed to a pool */
    qdata = malloc(sizeof(pfrq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc in io thread\n");
        exit(1);
    }

    qdata->type = PFRQ_NEWKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    qdata->db = db;
    qdata->genid = -1;
    if ((keylen > 0) && (keylen < MAXKEYLEN))
        memcpy(qdata->key, key, keylen);
    qdata->record = NULL;
    qdata->tag = NULL;
    qdata->broadcast = broadcast;
    qdata->dolocal = dolocal;
    qdata->flush = flush;

    qdata->opnum = opnum;
    qdata->helper_thread = helper_thread;
    qdata->seqnum = seqnum;

    /*fprintf(stderr, "enqueued PFRQ_DATA_KEY\n");*/

    rc = enque_pfault_ll(db->dbenv, qdata);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, genid   : enqueue a fault for a data record, as specified by
                            a table/genid  */
int enque_pfault_olddata(struct dbtable *db, unsigned long long genid, int opnum,
                         int helper_thread, unsigned int seqnum, int broadcast,
                         int dolocal, int flush)
{
    pfrq_t *qdata = NULL;
    int rc;

    /* XXX this should be changed to a pool */
    qdata = malloc(sizeof(pfrq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc in io thread\n");
        exit(1);
    }

    qdata->type = PFRQ_OLDDATA;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->record = NULL;
    qdata->tag = NULL;
    qdata->len = 0;
    qdata->broadcast = broadcast;
    qdata->dolocal = dolocal;
    qdata->flush = flush;

    /*fprintf(stderr, "enqueued PFRQ_DATA\n");*/

    qdata->opnum = opnum;
    qdata->helper_thread = helper_thread;
    qdata->seqnum = seqnum;

    rc = enque_pfault_ll(db->dbenv, qdata);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, genid   : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_pfault_olddata_oldkeys(struct dbtable *db, unsigned long long genid,
                                 int opnum, int helper_thread,
                                 unsigned int seqnum, int broadcast,
                                 int dolocal, int flush)
{
    pfrq_t *qdata = NULL;
    int rc;

    /* XXX this should be changed to a pool */
    qdata = malloc(sizeof(pfrq_t));
    if (qdata == NULL) {
        fprintf(stderr, "failed to malloc in io thread\n");
        exit(1);
    }

    qdata->type = PFRQ_OLDDATA_OLDKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->record = NULL;
    qdata->tag = NULL;
    qdata->len = 0;
    qdata->broadcast = broadcast;
    qdata->dolocal = dolocal;
    qdata->flush = flush;
    qdata->seqnum = seqnum;

    /*fprintf(stderr, "enqueued PFRQ_DATA_KEYS\n");*/

    qdata->opnum = opnum;
    qdata->helper_thread = helper_thread;

    rc = enque_pfault_ll(db->dbenv, qdata);

    if (rc != 0) {
        free(qdata);
    }

    return rc;
}

/* given a                      : enqueue a an op that
     table,genid,                 1) faults in dta by tbl/genid
     tag,record,reclen            2) forms all keys
                                  3) enqueues n ops to fault in each key
                                  4) forms new record by taking found record +
                                     tag/record/reclen
                                  5) forms all keys from new record.
                                  6) enqueues n ops to fault in each key.
                                  */
int enque_pfault_olddata_oldkeys_newkeys(struct dbtable *db,
                                         unsigned long long genid, char *tag,
                                         int taglen, void *record, int reclen,
                                         int opnum, int helper_thread,
                                         unsigned int seqnum, int broadcast,
                                         int dolocal, int flush)
{
    pfrq_t *qdata = NULL;
    int rc;

    /* XXX this should be changed to a pool */
    qdata = malloc(sizeof(pfrq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc in io thread\n");
        exit(1);
    }

    qdata->type = PFRQ_OLDDATA_OLDKEYS_NEWKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->record = malloc(reclen);
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->tag = malloc(taglen + 1);
    memcpy(qdata->tag, tag, taglen);
    qdata->tag[taglen] = '\0';
    qdata->taglen = taglen;
    qdata->broadcast = broadcast;
    qdata->dolocal = dolocal;
    qdata->flush = flush;

    /*fprintf(stderr, "enqueued PFRQ_DATA_KEYS_NEWKEYS\n");*/
    qdata->opnum = opnum;
    qdata->helper_thread = helper_thread;
    qdata->seqnum = seqnum;
    rc = enque_pfault_ll(db->dbenv, qdata);

    if (rc != 0) {
        free(qdata->tag);
        free(qdata->record);
        free(qdata);
    }
    return rc;
}

#if 0
/* just commenting out. don't want to figure out return type */
int enque_pfault_exit(struct dbenv *dbenv)
{
   pfrq_t *qdata=NULL;
   int rc; 

   /* XXX this should be changed to a pool */
   qdata = malloc(sizeof(pfrq_t));
   if (qdata == NULL)
   {
      fprintf(stderr, "failed to malloc in io thread\n");
      exit(1);
   }

   qdata->type   = PFRQ_EXITTHD;
   qdata->index  = -1;
   qdata->db     = NULL;
   qdata->genid  = -1;
   qdata->broadcast = 0;
   qdata->dolocal = 1;
   qdata->record = NULL;
   qdata->tag = NULL;
   rc = enque_pfault_ll(dbenv, qdata);
   
   if (rc != 0)
   {
      free(qdata);
   }
}
#endif

static int is_bad_rc(int rc)
{
    if (rc == 0)
        return 0;
    if (rc == 1)
        return 0;

    return 1;
}

static int lock_variable;

/* much of the code (most) here is stolen from record.c */
static void *prefault_io_thread(void *arg)
{
    comdb2_name_thread(__func__);
    struct dbenv *dbenv;
    int rc = 0, needfree = 0;
    pfrq_t *req;
    struct ireq iq;
    struct thread_info *thdinfo;
    unsigned char fldnullmap[32];

    thread_started("prefault io");

    init_fake_ireq(thedb, &iq);

    dbenv = arg;

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    logmsg(LOGMSG_INFO, "io thread started as %p\n", (void *)pthread_self());

    Pthread_setspecific(lockmgr_key, &lock_variable);

    /* thdinfo is assigned to thread specific variable thd_info_key which
     * will automatically free it when the thread exits. */
    thdinfo = malloc(sizeof(struct thread_info));
    if (thdinfo == NULL) {
        logmsg(LOGMSG_FATAL, "**aborting due malloc failure thd %p\n", (void *)pthread_self());
        abort();
    }
    thdinfo->uniquetag = 0;
    thdinfo->ct_id_key = 0LL;
    thdinfo->ct_add_table = NULL;
    thdinfo->ct_del_table = NULL;
    thdinfo->ct_add_index = NULL;
    Pthread_setspecific(thd_info_key, thdinfo);

    while (1) {
        req = NULL;

        Pthread_mutex_lock(&(dbenv->prefaultiopool.mutex));

        req = (pfrq_t *)queue_next(dbenv->prefaultiopool.ioq);

        while (req == NULL) {
            Pthread_cond_wait(&(dbenv->prefaultiopool.cond),
                              &(dbenv->prefaultiopool.mutex));

            req = (pfrq_t *)queue_next(dbenv->prefaultiopool.ioq);
        }

        needfree = 0;
        assert(req != NULL);

        /*fprintf(stderr, "consumed item, q now=%d\n",
           queue_count(dbenv->prefaultiopool.ioq));*/

        Pthread_mutex_unlock(&(dbenv->prefaultiopool.mutex));

        assert(req != NULL);

        bzero(fldnullmap, sizeof(unsigned char) * 32);

        MEMORY_SYNC;

        if (req->dolocal)
            switch (req->type) {
            /* fault in a dta record by genid */
            case PFRQ_OLDDATA: {
                int fndlen;
                int od_len;
                unsigned char fnddta[32768];

#ifdef PREFAULT_TRACE
                fprintf(stderr, "consumed PFRQ_DATA %lld\n", req->genid);
#endif

                dbenv->prefault_stats.num_prfq_data++;

                iq.usedb = req->db;

                /* XXX revisit this - bring in the record to malloced mem,
                   just to free it? */
                od_len = getdatsize(iq.usedb);

                if (req->helper_thread != -1) {
                    unsigned char *op_bitmap;
                    unsigned char *pfk_bitmap;
                    int i;

                    i = req->helper_thread;
                    if ((i < 0) || (i >= dbenv->prefault_helper.numthreads))
                        break;

                    pfk_bitmap = dbenv->prefault_helper.threads[i].pfk_bitmap;
                    op_bitmap = pfk_bitmap + req->opnum * 8;

/*
fprintf(stderr, "opnum %d btst(%x, %d)\n",
   req->opnum, op_bitmap, 64 + req->index);
*/

#ifdef PREFAULT_SKIP_SEQ
                    if ((req->seqnum) &&
                        (req->seqnum !=
                         dbenv->prefault_helper.threads[i].seqnum)) {
                        dbenv->prefault_stats.skipped_seq++;
                        break;
                    }
#endif

#ifdef PREFAULT_SKIP
                    if (btst(op_bitmap, 63)) {
                        dbenv->prefault_stats.skipped++;
                        break;
                    }
#endif
                }

                dbenv->prefault_stats.processed++;
                rc = ix_find_by_rrn_and_genid_dirty(&iq, 2, req->genid, fnddta,
                                                    &fndlen, od_len);

                break;
            }

            /* just fault in 1 key, no dta */
            case PFRQ_OLDKEY: {
                int fndrrn = 0;
                unsigned long long genid = 0;
                char fndkey[MAXKEYLEN];

#ifdef PREFAULT_TRACE
                fprintf(stderr, "consumed PFRQ_KEY(%d)\n", req->index);
#endif

                if ((req->index < 0) || (req->index > 49)) {
                    logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n",
                            req->index);
                    break;
                }

                dbenv->prefault_stats.num_prfq_key++;

                iq.usedb = req->db;

                if (req->helper_thread != -1) {
                    unsigned char *op_bitmap;
                    unsigned char *pfk_bitmap;
                    int i;

                    i = req->helper_thread;
                    if ((i < 0) || (i >= dbenv->prefault_helper.numthreads))
                        break;

                    pfk_bitmap = dbenv->prefault_helper.threads[i].pfk_bitmap;
                    op_bitmap = pfk_bitmap + req->opnum * 8;

#ifdef PREFAULT_SKIP_SEQ
                    if ((req->seqnum) &&
                        (req->seqnum !=
                         dbenv->prefault_helper.threads[i].seqnum)) {
                        dbenv->prefault_stats.skipped_seq++;
                        break;
                    }
#endif

#ifdef PREFAULT_SKIP
                    if (btst(op_bitmap, req->index)) {
                        dbenv->prefault_stats.skipped++;
                        break;
                    }
#endif
                }

                dbenv->prefault_stats.processed++;
                rc = ix_find_dirty(&iq, req->index, req->key, req->len, fndkey,
                                   &fndrrn, &genid, NULL, NULL, 0);

                break;
            }

            /* just fault in 1 key, no dta */
            case PFRQ_NEWKEY: {
                int fndrrn = 0;
                unsigned long long genid = 0;
                char fndkey[MAXKEYLEN];

#ifdef PREFAULT_TRACE
                fprintf(stderr, "consumed PFRQ_KEY(%d)\n", req->index);
#endif

                if ((req->index < 0) || (req->index > 49)) {
                    logmsg(LOGMSG_ERROR, "PFRQ_NEWKEY ix %d out of bounds\n",
                            req->index);
                    break;
                }

                dbenv->prefault_stats.num_prfq_key++;

                iq.usedb = req->db;

                if (req->helper_thread != -1) {
                    unsigned char *op_bitmap;
                    unsigned char *pfk_bitmap;
                    int i;

                    i = req->helper_thread;
                    if ((i < 0) || (i >= dbenv->prefault_helper.numthreads))
                        break;

                    pfk_bitmap = dbenv->prefault_helper.threads[i].pfk_bitmap;
                    op_bitmap = pfk_bitmap + req->opnum * 8;

/*
fprintf(stderr, "opnum %d btst(%x, %d)\n",
   req->opnum, op_bitmap, 64 + req->index);
*/

#ifdef PREFAULT_SKIP_SEQ
                    if ((req->seqnum) &&
                        (req->seqnum !=
                         dbenv->prefault_helper.threads[i].seqnum)) {
                        dbenv->prefault_stats.skipped_seq++;
                        break;
                    }
#endif

#ifdef PREFAULT_SKIP
                    MEMORY_SYNC;
                    if (btst(op_bitmap, 64 + req->index)) {
                        /*fprintf(stderr, "opnum %d btst(%x, %d)\n",
                          req->opnum, op_bitmap, 64 + req->index);*/

                        /*fprintf(stderr, "skipping\n");*/
                        dbenv->prefault_stats.skipped++;
                        break;
                    }
#endif
                }

                dbenv->prefault_stats.processed++;
                rc = ix_find_dirty(&iq, req->index, req->key, req->len, fndkey,
                                   &fndrrn, &genid, NULL, NULL, 0);

                break;
            }

            /* this op peforms 1 i/o faulting in a dta record by genid.
               it then enqueues n ops to fault in each key formed from that
               dta record */
            case PFRQ_OLDDATA_OLDKEYS: {
                size_t od_len;
                int od_len_int;
                int fndlen = 0;
                int ixnum = 0;
                unsigned char fnddta[32768];

#ifdef PREFAULT_TRACE
                fprintf(stderr, "consumed PFRQ_DATA_KEYS\n");
#endif
                dbenv->prefault_stats.num_prfq_data_keys++;

                iq.usedb = req->db;

                od_len_int = getdatsize(iq.usedb);
                if (od_len_int <= 0) {
                    break;
                }
                od_len = (size_t)od_len_int;

                if (req->helper_thread != -1) {
                    unsigned char *op_bitmap;
                    unsigned char *pfk_bitmap;
                    int i;

                    i = req->helper_thread;
                    if ((i < 0) || (i >= dbenv->prefault_helper.numthreads))
                        break;

                    pfk_bitmap = dbenv->prefault_helper.threads[i].pfk_bitmap;
                    op_bitmap = pfk_bitmap + req->opnum * 8;

#ifdef PREFAULT_SKIP_SEQ
                    if ((req->seqnum) &&
                        (req->seqnum !=
                         dbenv->prefault_helper.threads[i].seqnum)) {
                        dbenv->prefault_stats.skipped_seq++;
                        break;
                    }
#endif

#ifdef PREFAULT_SKIP
                    if (btst(op_bitmap, 63)) {
                        dbenv->prefault_stats.skipped++;
                        break;
                    }
#endif
                }

                dbenv->prefault_stats.processed++;
                rc = ix_find_by_rrn_and_genid_dirty(&iq, 2, req->genid, fnddta,
                                                    &fndlen, od_len);

                if ((is_bad_rc(rc)) || (od_len != fndlen)) {
                    break;
                }

                for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
                    char key[MAXKEYLEN];
                    int keysz = 0;
                    keysz = getkeysize(iq.usedb, ixnum);
                    if (keysz < 0) {
                        logmsg(LOGMSG_ERROR, "prefault_thd:cannot get key size"
                                             " tbl %s. idx %d\n",
                               iq.usedb->tablename, ixnum);
                        break;
                    }
                    rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)fnddta, key);
                    if (rc == -1) {
                        break;
                    }

                    rc = enque_pfault_oldkey(iq.usedb, key, keysz, ixnum,
                                             req->opnum, req->helper_thread,
                                             req->seqnum, 0, 1, 0);
                }

                break;
            }

            /*
               this op peforms 1 i/o faulting in a dta record by genid.
               it then enqueues n ops to fault in each key formed from that
               dta record

               it then forms a new record by unioning the found record + the
               enqueued record, then forms keys from this record and
               enqueues n ops to fault in each key formed from that record.
            */

            case PFRQ_OLDDATA_OLDKEYS_NEWKEYS: {
                size_t od_len;
                int od_len_int;
                int fndlen = 0;
                int ixnum = 0;
                struct convert_failure reason;
                struct schema *dynschema = NULL;
                char tag[MAXTAGLEN];
                u_char fnddta[32768];
                u_char od_dta[32768];

                needfree = 1;

#ifdef PREFAULT_TRACE
                fprintf(stderr, "consumed PFRQ_DATA_KEYS_NEWKEYS\n");
#endif
                dbenv->prefault_stats.num_prfq_data_keys_newkeys++;

                iq.usedb = req->db;
                rc = resolve_tag_name(&iq, req->tag, req->taglen, &dynschema,
                                      tag, sizeof(tag));
                if (rc != 0) {
                    /*fprintf(stderr, "couldnt resolve tagname '%*.*s'\n",
                      req->taglen, req->taglen, req->tag);*/
                    break;
                }
                od_len_int = getdatsize(iq.usedb);
                if (od_len_int <= 0) {
                    logmsg(LOGMSG_ERROR, "od_len_int = %d\n", od_len_int);
                    if (dynschema)
                        free_dynamic_schema(iq.usedb->tablename, dynschema);
                    break;
                }
                od_len = (size_t)od_len_int;

                /* fault old dta */
                if (req->helper_thread != -1) {
                    unsigned char *op_bitmap;
                    unsigned char *pfk_bitmap;
                    int i;

                    i = req->helper_thread;
                    if ((i < 0) || (i >= dbenv->prefault_helper.numthreads)) {
                        if (dynschema)
                            free_dynamic_schema(iq.usedb->tablename, dynschema);
                        break;
                    }

                    pfk_bitmap = dbenv->prefault_helper.threads[i].pfk_bitmap;
                    op_bitmap = pfk_bitmap + req->opnum * 8;

#ifdef PREFAULT_SKIP_SEQ
                    if ((req->seqnum) &&
                        (req->seqnum !=
                         dbenv->prefault_helper.threads[i].seqnum)) {
                        dbenv->prefault_stats.skipped_seq++;
                        if (dynschema)
                            free_dynamic_schema(iq.usedb->tablename, dynschema);
                        break;
                    }
#endif

#ifdef PREFAULT_SKIP
                    if (btst(op_bitmap, 63)) {
                        dbenv->prefault_stats.skipped++;
                        if (dynschema)
                            free_dynamic_schema(iq.usedb->tablename, dynschema);
                        break;
                    }
#endif
                }

                dbenv->prefault_stats.processed++;
                rc = ix_find_by_rrn_and_genid_dirty(&iq, 2, req->genid, fnddta,
                                                    &fndlen, od_len);

                if ((is_bad_rc(rc)) || (od_len != fndlen)) {
                    dbenv->prefault_stats
                        .num_prfq_data_keys_newkeys_no_olddta++;
                    if (dynschema)
                        free_dynamic_schema(iq.usedb->tablename, dynschema);

                    break;
                }

                /* enqueue faults for old keys */
                for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
                    char key[MAXKEYLEN];
                    int keysz = 0;
                    keysz = getkeysize(iq.usedb, ixnum);
                    if (keysz < 0) {
                        logmsg(LOGMSG_ERROR, "prefault_thd:cannot get key size"
                                             " tbl %s. idx %d\n",
                               iq.usedb->tablename, ixnum);
                        continue;
                    }
                    rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)fnddta, key);
                    if (rc == -1) {
                        logmsg(LOGMSG_ERROR,
                               "prefault_thd:cannot convert .ONDISK to IDX"
                               " %d of TBL %s\n",
                               ixnum, iq.usedb->tablename);
                        continue;
                    }

                    rc = enque_pfault_oldkey(iq.usedb, key, keysz, ixnum,
                                             req->opnum, req->helper_thread,
                                             req->seqnum, 0, 1, 0);
                }

                /*
                 * Form the new record in od_dta by taking the union of the
                 * old record and the changes.
                 */
                memcpy(od_dta, fnddta, od_len);
                rc = ctag_to_stag_buf(iq.usedb, tag, req->record,
                                      WHOLE_BUFFER, fldnullmap, ".ONDISK",
                                      od_dta, CONVERT_UPDATE, &reason);
                if (rc < 0) {
                    /* I really want to know why this happens... lets have the
                     * trace on a turnoffable switch */
                    if (gbl_prefault_verbose) {
                        char str[128];
                        convert_failure_reason_str(&reason, iq.usedb->tablename,
                                                   tag, ".ONDISK", str,
                                                   sizeof(str));
                        logmsg(LOGMSG_USER,
                               "%s: conv failed for %s:%s->.ONDISK rc %d\n",
                               __func__, iq.usedb->tablename, tag, rc);
                        logmsg(LOGMSG_USER, "%s: reason: %s\n", __func__, str);
                    }

                    if (dynschema)
                        free_dynamic_schema(iq.usedb->tablename, dynschema);
                    break;
                }

                /* enqueue faults for new keys */
                for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
                    char key[MAXKEYLEN];
                    int keysz = 0;
                    keysz = getkeysize(iq.usedb, ixnum);
                    if (keysz < 0) {
                        logmsg(LOGMSG_ERROR, "prefault_thd:cannot get key size"
                                             " tbl %s. idx %d\n",
                               iq.usedb->tablename, ixnum);
                        continue;
                    }
                    rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)od_dta, key);
                    if (rc == -1) {
                        logmsg(LOGMSG_ERROR,
                               "prefault_thd:cannot convert .ONDISK to IDX"
                               " %d of TBL %s\n",
                               ixnum, iq.usedb->tablename);
                        continue;
                    }

                    rc = enque_pfault_newkey(iq.usedb, key, keysz, ixnum,
                                             req->opnum, req->helper_thread,
                                             req->seqnum, 0, 1, 0);
                }

                if (dynschema)
                    free_dynamic_schema(iq.usedb->tablename, dynschema);
                break;
            }

            case PFRQ_EXITTHD: {
                backend_thread_event(dbenv, COMDB2_THR_EVENT_DONE_RDONLY);
                free(req);
                /*fprintf(stderr, "exiting prefault thread %p\n",
                  (void *)pthread_self());*/
                pthread_exit((void *)0);
                break;
            }

            default: {
                break;
            }
            }

        if (req->broadcast) {
            broadcast_prefault(dbenv, req);
        }
        if (needfree) {
            free(req->record);
            free(req->tag);
        }
        free(req);
    }
}

void prefault_stats(struct dbenv *dbenv)
{
    logmsg(LOGMSG_USER, "num_add_record %d\n",
            dbenv->prefault_stats.num_add_record);
    logmsg(LOGMSG_USER, "num_del_record %d\n",
            dbenv->prefault_stats.num_del_record);
    logmsg(LOGMSG_USER, "num_upd_record %d\n",
            dbenv->prefault_stats.num_upd_record);

    logmsg(LOGMSG_USER, "num_prfq_data %d\n", dbenv->prefault_stats.num_prfq_data);
    logmsg(LOGMSG_USER, "num_prfq_key %d\n", dbenv->prefault_stats.num_prfq_key);
    logmsg(LOGMSG_USER, "num_prfq_data_keys %d\n",
            dbenv->prefault_stats.num_prfq_data_keys);
    logmsg(LOGMSG_USER, "num_prfq_data_keys_newkeys %d\n",
            dbenv->prefault_stats.num_prfq_data_keys_newkeys);

    logmsg(LOGMSG_USER, "num_prfq_data_broadcast %d\n",
            dbenv->prefault_stats.num_prfq_data_broadcast);
    logmsg(LOGMSG_USER, "num_prfq_key_broadcast %d\n",
            dbenv->prefault_stats.num_prfq_key_broadcast);
    logmsg(LOGMSG_USER, "num_prfq_data_keys_broadcast %d\n",
            dbenv->prefault_stats.num_prfq_data_keys_broadcast);
    logmsg(LOGMSG_USER, "num_prfq_data_keys_newkeys_broadcast %d\n",
            dbenv->prefault_stats.num_prfq_data_keys_newkeys_broadcast);

    logmsg(LOGMSG_USER, "num_prfq_data_received %d\n",
            dbenv->prefault_stats.num_prfq_data_received);
    logmsg(LOGMSG_USER, "num_prfq_key_received %d\n",
            dbenv->prefault_stats.num_prfq_key_received);
    logmsg(LOGMSG_USER, "num_prfq_data_keys_received %d\n",
            dbenv->prefault_stats.num_prfq_data_keys_received);
    logmsg(LOGMSG_USER, "num_prfq_data_keys_newkeys_received %d\n",
            dbenv->prefault_stats.num_prfq_data_keys_newkeys_received);

    logmsg(LOGMSG_USER, "num_prfq_data_keys_newkeys_no_olddta %d\n",
            dbenv->prefault_stats.num_prfq_data_keys_newkeys_no_olddta);

    logmsg(LOGMSG_USER, "num_ioq_full %d\n", dbenv->prefault_stats.num_ioq_full);
    logmsg(LOGMSG_USER, "num_nohelpers %d\n", dbenv->prefault_stats.num_nohelpers);

    logmsg(LOGMSG_USER, "skipped %d\n", dbenv->prefault_stats.skipped);

    logmsg(LOGMSG_USER, "skipped_seq %d\n", dbenv->prefault_stats.skipped_seq);

    logmsg(LOGMSG_USER, "processed %d\n", dbenv->prefault_stats.processed);

    logmsg(LOGMSG_USER, "aborts %d\n", dbenv->prefault_stats.aborts);
}

void prefault_kill_bits(struct ireq *iq, int ixnum, int type)
{
    /* light the prefault kill bit for this subop - oldkeys */
    if ((iq->blkstate) && (iq->blkstate->pfk_bitmap)) {
        unsigned char *op_bitmap;
        int bit = -1;

        op_bitmap = iq->blkstate->pfk_bitmap + iq->blkstate->opnum * 8;

        if (ixnum != -1) {
            if ((iq->blkstate->pfkseq ==
                 iq->dbenv->prefault_helper.threads[iq->helper_thread]
                     .seqnum) &&
                (iq->blkstate->opnum > 0) &&
                (iq->blkstate->opnum < gbl_maxblockops) && (ixnum >= 0) &&
                (ixnum < 50)) {
                if (type == PFRQ_OLDKEY)
                    bit = ixnum;
                else
                    bit = 64 + ixnum;
            }
        } else {
            if ((iq->blkstate->pfkseq ==
                 iq->dbenv->prefault_helper.threads[iq->helper_thread]
                     .seqnum) &&
                (iq->blkstate->opnum > 0) &&
                (iq->blkstate->opnum < gbl_maxblockops))
                bit = 63;
        }

        if (bit != -1) {
            bset(op_bitmap, bit);
            MEMORY_SYNC;
        }
    }
}
