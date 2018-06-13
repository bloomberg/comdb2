/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include "limit_fortify.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <plbitlib.h> /* for bset/btst */
#include <segstring.h>
#include "nodemap.h"
#include <logmsg.h>

pthread_key_t bdb_key;
pthread_key_t lock_key;

const char *readlockstr = "READLOCK";
const char *writelockstr = "WRITELOCK";

static void bdb_stack_unpack(bdb_state_type *bdb_state, DBT *data);

void print(bdb_state_type *bdb_state, char *format, ...)
{
    va_list ap;

    va_start(ap, format);

    if (bdb_state && bdb_state->callback && bdb_state->callback->print_rtn)
        (bdb_state->callback->print_rtn)(format, ap);
    else
        logmsgvf(LOGMSG_USER, stderr, format, ap);

    va_end(ap);
}

extern bdb_state_type *gbl_bdb_state;

void bdb_set_key(bdb_state_type *bdb_state)
{
    int rc;

    if (gbl_bdb_state)
        return;

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    gbl_bdb_state = bdb_state;

    rc = pthread_setspecific(bdb_key, bdb_state);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "pthread_setspecific failed\n");
    }
}

void *mymalloc(size_t size)
{
    /*
    if (size > 100000)
       fprintf(stderr, "mymalloc: size = %d\n", (int)size);
    */

    return (malloc(size));
}

void myfree(void *ptr)
{
    /*char *cptr;*/

    /*fprintf(stderr, "mymalloc: size = %d\n", (int)size);*/
    /* TODO why did we do this? seems unsafe since we sometimes malloc 0 len */
    /*cptr = (char *)ptr;*/
    /*cptr[0] = 0;*/

    free(ptr);
}

void *myrealloc(void *ptr, size_t size)
{
    /*fprintf(stderr, "myrealloc: size = %d\n", (int)size);*/

    return (realloc(ptr, size));
}

/* retrieve the user pointer associated with a bdb_handle */
void *bdb_get_usr_ptr(bdb_state_type *bdb_state) { return bdb_state->usr_ptr; }

char *bdb_strerror(int error)
{
    switch (error) {
    case DB_ODH_CORRUPT:
        return "Ondisk header corrupt";
    default:
        return db_strerror(error);
    }
}

extern long long time_micros(void);

int bdb_amimaster(bdb_state_type *bdb_state)
{
    /*
    ** if (bdb_state->repinfo->master_eid ==
    *net_get_mynode(bdb_state->repinfo->netinfo))
    **    return 1;
    ** else
    **    return 0;
    */

    repinfo_type *repinfo = bdb_state->repinfo;
    return repinfo->master_host == repinfo->myhost;
}

char *bdb_whoismaster(bdb_state_type *bdb_state)
{
    if (bdb_state->repinfo->master_host != db_eid_invalid)
        return bdb_state->repinfo->master_host;
    else
        return NULL;
}

int bdb_get_rep_master(bdb_state_type *bdb_state, char **master_out,
                       uint32_t *gen, uint32_t *egen)
{
    return bdb_state->dbenv->get_rep_master(bdb_state->dbenv, master_out, gen,
                                            egen);
}

int bdb_get_sanc_list(bdb_state_type *bdb_state, int max_nodes,
                      const char *nodes[REPMAX])
{

    return net_get_sanctioned_node_list(bdb_state->repinfo->netinfo, max_nodes,
                                        nodes);
}

int bdb_seqnum_compare(void *inbdb_state, seqnum_type *seqnum1,
                       seqnum_type *seqnum2)
{
    bdb_state_type *bdb_state = (bdb_state_type *)inbdb_state;
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->attr->enable_seqnum_generations &&
        seqnum1->generation != seqnum2->generation)
        return (seqnum1->generation < seqnum2->generation ? -1 : 1);
    return log_compare(&(seqnum1->lsn), &(seqnum2->lsn));
}

char *bdb_format_seqnum(const seqnum_type *seqnum, char *buf, size_t bufsize)
{
    const DB_LSN *lsn = (const DB_LSN *)seqnum;
    snprintf(buf, bufsize, "%u:%u", (unsigned)lsn->file, (unsigned)lsn->offset);
    return buf;
}

int bdb_get_seqnum(bdb_state_type *bdb_state, seqnum_type *seqnum)
{
    DB_LSN our_lsn;
    DB_LOG_STAT *log_stats;
    int outrc;

    BDB_READLOCK("bdb_get_seqnum");

    /* XXX this continues to be retarted.  there has to be a lighter weight
       way to get the lsn */
    bzero(seqnum, sizeof(seqnum_type));
    bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats, 0);
    if (log_stats) {
        make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
        free(log_stats);
        memcpy(seqnum, &our_lsn, sizeof(DB_LSN));
        outrc = 0;
    } else {
        outrc = -1;
    }
    BDB_RELLOCK();
    return outrc;
}

int bdb_get_lsn(bdb_state_type *bdb_state, int *logfile, int *offset)
{
    DB_LSN outlsn;
    __log_txn_lsn(bdb_state->dbenv, &outlsn, NULL, NULL);
    *logfile = outlsn.file;
    *offset = outlsn.offset;
    return 0;
}

int bdb_get_lsn_node(bdb_state_type *bdb_state, char *host, int *logfile,
                     int *offset)
{
    *logfile = bdb_state->seqnum_info->seqnums[nodeix(host)].lsn.file;
    *offset = bdb_state->seqnum_info->seqnums[nodeix(host)].lsn.offset;
    return 0;
}

void bdb_make_seqnum(seqnum_type *seqnum, uint32_t logfile, uint32_t logbyte)
{
    DB_LSN lsn;
    lsn.file = logfile;
    lsn.offset = logbyte;
    bzero(seqnum, sizeof(seqnum_type));
    memcpy(seqnum, &lsn, sizeof(DB_LSN));
}

void bdb_get_txn_stats(bdb_state_type *bdb_state, int *txn_commits)
{
    DB_TXN_STAT *txn_stats;

    BDB_READLOCK("bdb_get_txn_stats");

    bdb_state->dbenv->txn_stat(bdb_state->dbenv, &txn_stats, 0);
    *txn_commits = txn_stats->st_ncommits;

    BDB_RELLOCK();

    free(txn_stats);
}

void bdb_get_cache_stats(bdb_state_type *bdb_state, uint64_t *hits,
                         uint64_t *misses, uint64_t *reads, uint64_t *writes,
                         uint64_t *thits, uint64_t *tmisses)
{
    DB_MPOOL_STAT *mpool_stats;

    BDB_READLOCK("bdb_get_cache_stats");
    bdb_state->dbenv->memp_stat(bdb_state->dbenv, &mpool_stats, NULL,
                                DB_STAT_MINIMAL);

    /* We find leaf pages only a more useful metric. */
    *hits = mpool_stats->st_cache_lhit;
    *misses = mpool_stats->st_cache_lmiss;
    if (reads)
        *reads = mpool_stats->st_page_in;
    if (writes)
        *writes = mpool_stats->st_page_out;

    free(mpool_stats);

    bdb_temp_table_stat(bdb_state, &mpool_stats);
    if (thits)
        *thits = mpool_stats->st_cache_hit;
    if (tmisses)
        *tmisses = mpool_stats->st_cache_miss;

    BDB_RELLOCK();

    free(mpool_stats);
}

void add_dummy(bdb_state_type *bdb_state)
{
    if (bdb_state->exiting)
        return;

    int rc = bdb_add_dummy_llmeta();
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s %s bdb_add_dummy_llmeta rc: %d\n", __FILE__,
                __func__, rc);
    }
    bdb_state->repinfo->repstats.dummy_adds++;

#if 0
   int rc;


   if (bdb_state->parent)
      bdb_state = bdb_state->parent;

   
   /*rc = bdb_state->dbenv->rep_flush(bdb_state->dbenv);*/

   /* force a checkpoint to occur no matter what */
   rc = bdb_state->dbenv->txn_checkpoint(bdb_state->dbenv, 0, 0, DB_FORCE);
   if (rc != 0)
   {
      fprintf(stderr, "txn_checkpoint err %d\n", rc);
   }
   bdb_state->repinfo->repstats.dummy_adds++;

   return;
#endif
}

int bdb_dump_dta_file_n(bdb_state_type *bdb_state, int dtanum, FILE *out)
{
/* TODO */
#if 0
   DBC *dbcp;
   int rc;
   int count;
   DB *db;

   /* For dtastripe, dump each stripe file in turn. */
   if (bdb_state->attr->dtastripe && dtanum == 0)
   {
      int ii;
      for (rc = 0, ii = 0; ii < bdb_state->attr->dtastripe; ii++)
      {
         fprintf(out, "DTASTRIPE %d:-\n", ii);
         if(bdb_dump_dta_file_n(bdb_state, -1 - ii, out) != 0)
            rc = -1;
      }
      return rc;
   }

   if (dtanum < 0)
   {
      dtanum = -1 - dtanum;
      if (dtanum < 0 || dtanum >= bdb_state->attr->dtastripe)
      {
         fprintf(stderr, "bdb_dump_dta_file_n: striped dtanum out of range\n");
         return -1;
      }
      db = get_dbp_from_genid(bdb_state, dtanum, 
      db = bdb_state->dbp_dtastripe[dtanum];
   }
   else
   {
      if((unsigned)dtanum > (unsigned)bdb_state->numdtafiles)
      {
         fprintf(stderr, "bdb_dump_dta_file_n: dtanum out of range\n");
         return -1;
      }
      db = bdb_state->dbp_dta[dtanum];
   }

   rc = db->cursor(db, NULL, &dbcp, 0);
   if(rc != 0)
   {
      fprintf(stderr, "bdb_dump_dta_file_n: cursor open failed %d\n", rc);
      return -1;
   }

   for(count = 0; ; count++)
   {
      DBT key, data;
      int rrn = 0;
      unsigned long long genid = 0;

      bzero(&key, sizeof(key));
      bzero(&data, sizeof(data));
      key.flags = DB_DBT_MALLOC;
      data.flags = DB_DBT_MALLOC;

      rc = dbcp->c_get(dbcp, &key, &data, DB_NEXT);
      if(rc != 0)
         break;

      if(bdb_state->attr->dtastripe)
      {
         rrn = 2;
         if(key.size == 8)
            genid = *((unsigned long long *)key.data);
      }
      else
      {
         if(key.size == 4 && key.data)
            rrn = *((int *)key.data);
         if(data.size >= 8 && data.data)
            genid = *((unsigned long long *)data.data);
      }
      fprintf(out, "%d: key %d rrn %d data %d genid 0x%llx\n",
            count, key.size, rrn, data.size, genid);

      if(key.data)
         free(key.data);
      if(data.data)
         free(data.data);
   }
   fprintf(out, "%d records retrieved, last rcode %d\n", count, rc);
   dbcp->c_close(dbcp);
   
   return 0;
#endif
    return 0;
}

bdbtype_t bdb_get_type(bdb_state_type *bdb_state)
{
    if (bdb_state)
        return bdb_state->bdbtype;
    else
        return BDBTYPE_NONE;
}

int bdb_zap_freerec(bdb_state_type *bdb_state, int *bdberr)
{
    *bdberr = BDBERR_MISC;
    return -1;
}

int bdb_set_timeout(bdb_state_type *bdb_state, unsigned int timeout,
                    int *bdberr)
{
    int rc;
    unsigned int tm;

#if 0
    int
    DB_ENV->get_timeout(DB_ENV *dbenv, db_timeout_t *timeoutp, u_int32_t flag);

    int
    DB_ENV->set_timeout(DB_ENV *dbenv, db_timeout_t timeout, u_int32_t flags);
#endif

    *bdberr = 0;
    bdb_state->dbenv->get_timeout(bdb_state->dbenv, &tm, DB_SET_LOCK_TIMEOUT);
    logmsg(LOGMSG_DEBUG, "MP: %u\n", tm);

    rc = bdb_state->dbenv->set_timeout(bdb_state->dbenv, timeout,
                                       DB_SET_LOCK_TIMEOUT);
    if (rc) {
        *bdberr = rc;
        return -1;
    }

    return rc;
}

void bdb_tran_set_usrptr(tran_type *tran, void *usr) { tran->usrptr = usr; }

void *bdb_tran_get_usrptr(tran_type *tran) { return tran->usrptr; }

/* Get first data item in btree.
 *
 * dtalenwanted - if 1, return length of data, of 0 return length of key.
 *
 * findrrn2 - if 1 we are looking at data file, skip rrn 1 (it contains meta
 *  data).
 */
static int bdb_get_first_data_length_dbp(bdb_state_type *bdb_state, DB *dbp,
                                         int dtalenwanted, int findrrn2,
                                         int *bdberr)
{
    DBT dbt_key, dbt_data;
    DBC *dbcp;
    int rc;
    int foundit;
    uint8_t ver;

    if (dbp == NULL) {
        logmsg(LOGMSG_ERROR, "bdb_get_first_data_length_dbp: dbp==NULL?!\n");
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* Open a cursor on this data file. */
    rc = dbp->cursor(dbp, NULL, &dbcp, 0);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
            *bdberr = BDBERR_DEADLOCK;
            break;

        default:
            logmsg(LOGMSG_ERROR, 
                    "bdb_get_first_data_length_dbp: cursor open failed %d %s\n",
                    rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    foundit = 0;
    while (!foundit) {
        /* Get the first thing in the database.  */
        bzero(&dbt_key, sizeof(dbt_key));
        bzero(&dbt_data, sizeof(dbt_data));

        dbt_key.flags = DB_DBT_MALLOC;
        dbt_data.flags = DB_DBT_MALLOC;

        rc = bdb_cget_unpack(bdb_state, dbcp, &dbt_key, &dbt_data, &ver,
                             DB_NEXT);

        if (rc != 0) {
            switch (rc) {
            case DB_LOCK_DEADLOCK:
            case DB_REP_HANDLE_DEAD:
                *bdberr = BDBERR_DEADLOCK;
                break;

            case DB_NOTFOUND:
                *bdberr = BDBERR_FETCH_DTA;
                break;

            default:
                logmsg(LOGMSG_ERROR, 
                        "bdb_get_first_data_length_dbp: read failed %d %s\n",
                        rc, db_strerror(rc));
                *bdberr = BDBERR_MISC;
                break;
            }
            dbcp->c_close(dbcp);
            return -1;
        }

        if (findrrn2 && dbt_key.size == sizeof(int) &&
            *((int *)dbt_key.data) < 2)
            foundit = 0;
        else
            foundit = 1;

        if (dbt_key.data)
            free(dbt_key.data);
        if (dbt_data.data)
            free(dbt_data.data);
    }
    dbcp->c_close(dbcp);

    /* Return the length of the data/key item.  Caller will process it to remove
     * genid length or whatever. */
    return dtalenwanted ? dbt_data.size : dbt_key.size;
}

static int bdb_get_first_data_length_int(bdb_state_type *bdb_state, int *bdberr)
{
    *bdberr = BDBERR_NOERROR;

    if (bdb_state->bdbtype != BDBTYPE_TABLE) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_state->attr->dtastripe > 0) {
        int stripen, length;

        /* Look at each stripe file until we find some data. */
        for (stripen = 0; stripen < bdb_state->attr->dtastripe; stripen++) {
            length = bdb_get_first_data_length_dbp(
                bdb_state, bdb_state->dbp_data[stripen][0], 1, 1, bdberr);

            if (length >= 0 || *bdberr != BDBERR_FETCH_DTA)
                break;
        }

        /* No genid adjustments needed */
        return length;
    } else {
        int length;

        length = bdb_get_first_data_length_dbp(
            bdb_state, bdb_state->dbp_data[0][0], 1, 1, bdberr);

        if (length >= 0) {
            /* Deduct length of genids */
            if (bdb_state->attr->genids) {
                length -= 8;
                if (length < 0) {
                    logmsg(LOGMSG_ERROR, "bdb_get_first_data_length_int: genid "
                                    "adjusted length is %d\n",
                            length);
                    length = 0;
                }
            }
        }

        return length;
    }
}

int bdb_get_first_data_length(bdb_state_type *bdb_state, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_get_first_data_length");
    rc = bdb_get_first_data_length_int(bdb_state, bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_get_first_index_length_int(bdb_state_type *bdb_state, int ixnum,
                                   int *bdberr)
{
    int length;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->bdbtype != BDBTYPE_TABLE) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    length = bdb_get_first_data_length_dbp(bdb_state, bdb_state->dbp_ix[ixnum],
                                           0, 0, bdberr);

    if (length >= 0) {
        if (bdb_keycontainsgenid(bdb_state, ixnum)) {
            if (bdb_state->attr->dtastripe > 0)
                length -= 8; /* subtract genid length */
            else
                length -= 4; /* subtract rrn length */
        }
    }

    return length;
}

int bdb_get_first_index_length(bdb_state_type *bdb_state, int ixnum,
                               int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_get_first_index_length");
    rc = bdb_get_first_index_length_int(bdb_state, ixnum, bdberr);
    BDB_RELLOCK();

    return rc;
}

void bdb_start_request(bdb_state_type *bdb_state)
{
    BDB_READLOCK("bdb_start_request");
}

void bdb_end_request(bdb_state_type *bdb_state) { BDB_RELLOCK(); }

void bdb_start_exclusive_request(bdb_state_type *bdb_state)
{
    BDB_WRITELOCK("bdb_start_exclusive_request");
}

void bdb_sql_dumpit(bdb_state_type *bdb_state) {}

void bdb_dumpit(bdb_state_type *bdb_state) {}

#if 0
static unsigned long long get_hrtimestamp(void)
{
#if defined(_IBM_SOURCE)

   timebasestruct_t hr_time;
   unsigned long long absolute_time;
   
   read_real_time(&hr_time, sizeof (struct timebasestruct));
   time_base_to_time(&hr_time, sizeof (struct timebasestruct));
   
   absolute_time =
    ((unsigned long long)hr_time.tb_high) *  1000000000
    + ((long long)hr_time.tb_low);
   
   return absolute_time;

#elif defined(_SUN_SOURCE)

   return 0;

#endif
}
#endif

void bdb_bulkdumpit(bdb_state_type *bdb_state) {}

/* Call this at the beginning of a request to reset all our per thread stats. */
void bdb_reset_thread_stats(void)
{
#ifdef BERKDB_4_2
    bb_berkdb_thread_stats_reset();
#endif
}

/* Call this at the end of a request to get our stats. */
const struct bdb_thread_stats *bdb_get_thread_stats(void)
{
#ifdef BERKDB_4_2
    return (const struct bdb_thread_stats *)bb_berkdb_get_thread_stats();
#else
    static struct bdb_thread_stats zero = {0};
    return &zero;
#endif
}

/* Call this any time to get process wide stats (which get updated locklessly)
 */
const struct bdb_thread_stats *bdb_get_process_stats(void)
{
#ifdef BERKDB_4_2
    return (const struct bdb_thread_stats *)bb_berkdb_get_process_stats();
#else
    static struct bdb_thread_stats zero = {0};
    return &zero;
#endif
}

/* Report bdb stats into the given logging function. */
void bdb_print_stats(const struct bdb_thread_stats *st, const char *prefix,
                     int (*printfn)(const char *, void *), void *context)
{
    char s[128];
    if (st->n_lock_waits > 0) {
        snprintf(s, sizeof(s), "%s%u lock waits took %u ms (%u ms/wait)\n",
                 prefix, st->n_lock_waits, U2M(st->lock_wait_time_us),
                 U2M(st->lock_wait_time_us / st->n_lock_waits));
        printfn(s, context);
    }
    if (st->n_preads > 0) {
        snprintf(s, sizeof(s), "%s%u preads took %u ms total of %u bytes\n",
                 prefix, st->n_preads, U2M(st->pread_time_us), st->pread_bytes);
        printfn(s, context);
    }
    if (st->n_pwrites > 0) {
        snprintf(s, sizeof(s), "%s%u pwrites took %u ms total of %u bytes\n",
                 prefix, st->n_pwrites, U2M(st->pwrite_time_us),
                 st->pwrite_bytes);
        printfn(s, context);
    }
    if (st->n_memp_fgets > 0) {
        snprintf(s, sizeof(s), "%s%u __memp_fget calls took %u ms\n", prefix,
                 st->n_memp_fgets, U2M(st->memp_fget_time_us));
        printfn(s, context);
    }
    if (st->n_memp_pgs > 0) {
        snprintf(s, sizeof(s), "%s%u __memp_pg calls took %u ms\n", prefix,
                 st->n_memp_pgs, U2M(st->memp_pg_time_us));
        printfn(s, context);
    }
    if (st->n_shallocs > 0 || st->n_shalloc_frees > 0) {
        snprintf(s, sizeof(s),
                 "%s%u shallocs took %u ms, %u shalloc_frees took %u ms\n",
                 prefix, st->n_shallocs, U2M(st->shalloc_time_us),
                 st->n_shalloc_frees, U2M(st->shalloc_free_time_us));
        printfn(s, context);
    }
}

void bdb_fprintf_stats(const struct bdb_thread_stats *st, const char *prefix,
                       FILE *out)
{
    bdb_print_stats(st, "  ", (int (*)(const char *, void *))fputs, out);
}

void bdb_register_rtoff_callback(bdb_state_type *bdb_state, void (*func)(void))
{
    bdb_state->signal_rtoff = func;
}

bdb_state_type *bdb_get_table_by_name(bdb_state_type *bdb_state, char *table)
{
    bdb_state_type *parent = bdb_state;
    int i;

    if (bdb_state->parent)
        parent = bdb_state->parent;

    for (i = 0; i < parent->numchildren; i++) {
        bdb_state_type *child;
        child = parent->children[i];
        if (child) {
            if (strcmp(child->name, table) == 0)
                return child;
        }
    }
    return NULL;
}

bdb_state_type *bdb_get_table_by_name_dbnum(bdb_state_type *bdb_state,
                                            char *table, int *dbnum)
{
    bdb_state_type *parent = bdb_state;
    int i;

    if (bdb_state->parent)
        parent = bdb_state->parent;

    for (i = 0; i < parent->numchildren; i++) {
        bdb_state_type *child;
        child = parent->children[i];
        if (child) {
            if (strcmp(child->name, table) == 0) {
                *dbnum = i;
                return child;
            }
        }
    }
    return NULL;
}

void bdb_lockspeed(bdb_state_type *bdb_state)
{
    u_int32_t lid;
    DB_ENV *dbenv = bdb_state->dbenv;
    int i;
    DB_LOCK lock;
    DBT lkname = {0};
    int start, end;

    dbenv->lock_id(dbenv, &lid);
    lkname.data = "hello";
    lkname.size = strlen("hello");
    start = comdb2_time_epochms();
    for (i = 0; i < 100000000; i++) {
        dbenv->lock_get(dbenv, lid, 0, &lkname, DB_LOCK_READ, &lock);
        dbenv->lock_put(dbenv, &lock);
    }
    end = comdb2_time_epochms();
    logmsg(LOGMSG_USER, "berkeley took %dms (%d per second)\n", end - start,
           100000000 / (end - start) * 1000);
    dbenv->lock_id_free(dbenv, lid);
}

void bdb_dumptrans(bdb_state_type *bdb_state)
{
    extern void berkdb_dumptrans(DB_ENV *);
    berkdb_dumptrans(bdb_state->dbenv);
}

void bdb_berkdb_iomap_set(bdb_state_type *bdb_state, int onoff)
{
    bdb_state->dbenv->setattr(bdb_state->dbenv, "iomap", NULL, onoff);
}

int bdb_berkdb_set_attr(bdb_state_type *bdb_state, char *attr, char *value,
                        int ivalue)
{
    char *optname;
    int rc;

    rc = bdb_state->dbenv->setattr(bdb_state->dbenv, attr, value, ivalue);
    return rc;
}

int bdb_berkdb_set_attr_after_open(bdb_attr_type *bdb_attr, char *attr,
                                   char *value, int ivalue)
{
    struct deferred_berkdb_option *opt;
    opt = malloc(sizeof(struct deferred_berkdb_option));
    opt->attr = strdup(attr);
    opt->value = strdup(value);
    opt->ivalue = ivalue;
    listc_abl(&bdb_attr->deferred_berkdb_options, opt);

    return 0;
}

void bdb_berkdb_dump_attrs(bdb_state_type *bdb_state, FILE *f)
{
    bdb_state->dbenv->dumpattrs(bdb_state->dbenv, f);
}

int bdb_berkdb_blobmem_yield(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->blobmem_yield(bdb_state->dbenv);
}

int bdb_recovery_start_lsn(bdb_state_type *bdb_state, char *lsnout, int lsnlen)
{
    DB_LSN lsn;
    int rc;
    rc = bdb_state->dbenv->get_recovery_lsn(bdb_state->dbenv, &lsn);
    if (rc)
        return rc;
    snprintf(lsnout, lsnlen, "%u:%u", lsn.file, lsn.offset);
    return 0;
}

int bdb_panic(bdb_state_type *bdb_state)
{
    extern int __db_panic(DB_ENV * dbenv, int err);
    __db_panic(bdb_state->dbenv, EINVAL);
    /* this shouldn't return!!! */
    return 0;
}
