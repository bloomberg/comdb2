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

/*
 * This module is intended for all file related bdb stuff e.g. opening,
 * closing, creating database, renaming databases etc.  This is all stuff that
 * I've refactored out of bdb.c to try to make that monster a little more
 * manageable.
 */

/* To get POSIX standard readdir_r() */
#ifdef _SUN_SOURCE
#ifndef _POSIX_PTHREAD_SEMANTICS
#define _POSIX_PTHREAD_SEMANTICS
#endif
#endif

#include "flibc.h"

#include <alloca.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>

#ifdef __sun
/* for PTHREAD_STACK_MIN on Solaris */
#define __EXTENSIONS__
#endif
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
#include <sys/statvfs.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <unistd.h>
#include <stddef.h>
#include <ctype.h>
#include <dirent.h>
#include <signal.h>
#include <assert.h>
#include <poll.h>
#include <libgen.h>

#include <str0.h>

#include <build/db.h>
#include <epochlib.h>

#include <net.h>
#include <cheapstack.h>
#include "bdb_int.h"
#include "locks.h"
#include "locks_wrap.h"
#include <time.h>
#include <ctrace.h>
#include <list.h>
#include "nodemap.h"
#include "intern_strings.h"
#include <list.h>

#include "debug_switches.h"

#include "endian_core.h"
#include "bdb_osqltrn.h"

#include "printformats.h"
#include "util.h"
#include <bb_oscompat.h>
#include <logmsg.h>

#include <build/db_int.h>
#include "dbinc/db_swap.h"

#include <dbinc/db_am.h>

#include "dbinc/log.h"
#include "dbinc/txn.h"
#include <rep_qstat.h>

#include <bdb_queuedb.h>
#include <schema_lk.h>
#include <tohex.h>
#include <timer_util.h>

#include <phys_rep.h>
#include <phys_rep_lsn.h>

extern int gbl_bdblock_debug;
extern int gbl_keycompr;
extern int gbl_early;
extern int gbl_exit;
extern int gbl_fullrecovery;
extern char *gbl_myhostname;
extern size_t gbl_blobmem_cap;
extern int gbl_backup_logfiles;
extern int gbl_commit_lsn_map;

#define FILENAMELEN 100

#define USE_GENID_IN_QUEUEDB_FILE_NAME() \
    (gbl_queuedb_genid_filename || (gbl_queuedb_file_threshold > 0))

extern int is_db_roomsync();
extern int get_schema_change_in_progress(const char *func, int line);

int gbl_debug_children_lock = 0;
int gbl_queuedb_genid_filename = 1;
int gbl_queuedb_file_threshold = 0;
int gbl_queuedb_file_interval = 60000;
static const char NEW_PREFIX[] = "new.";

static pthread_once_t ONCE_LOCK = PTHREAD_ONCE_INIT;

static int notclosingdta_trace = 0;

int rep_caught_up(bdb_state_type *bdb_state);
static int bdb_del_file(bdb_state_type *bdb_state, DB_TXN *tid, char *filename,
                        int *bdberr);
static int bdb_free_int(bdb_state_type *bdb_state, bdb_state_type *replace,
                        int *bdberr);

static int bdb_close_only_int(bdb_state_type *bdb_state, DB_TXN *tid,
                              int *bdberr);

enum {
    BDB_CLOSE_FLAGS_FLUSH = 1,
};
static int bdb_close_only_flags(bdb_state_type *, DB_TXN *, int *bdberr, int flags);

int bdb_rename_file(bdb_state_type *bdb_state, DB_TXN *tid, char *oldfile,
                    char *newfile, int *bdberr);

static int bdb_reopen_int(bdb_state_type *bdb_state);
static int open_dbs(bdb_state_type *, int, int, int, DB_TXN *, uint32_t);
static int close_dbs(bdb_state_type *bdb_state);
static int close_dbs_txn(bdb_state_type *bdb_state, DB_TXN *tid);
static int close_dbs_flush(bdb_state_type *bdb_state);
static int bdb_watchdog_test_io_dir(bdb_state_type *bdb_state, char *dir);

int __txn_commit_map_delete_logfile_txns(DB_ENV *, int);

void berkdb_set_recovery(DB_ENV *dbenv);
void watchdog_set_alarm(int seconds);
void watchdog_cancel_alarm(void);
const char *get_sc_to_name(const char *name);

extern void *lwm_printer_thd(void *p);
unsigned int sc_get_logical_redo_lwm();
/* Sorry - reaching into berkeley "internals" here.  This should
 * probably be an environment method. */
extern int __db_find_recovery_start_if_enabled(DB_ENV *dbenv, DB_LSN *lsn);
extern void *master_lease_thread(void *arg);
extern void *coherency_lease_thread(void *arg);

LISTC_T(struct checkpoint_list) ckp_lst;
static pthread_mutex_t ckp_lst_mtx;
int ckp_lst_ready = 0;
extern int gbl_set_seqnum_trace;

void bdb_checkpoint_list_init()
{
    listc_init(&ckp_lst, offsetof(struct checkpoint_list, lnk));
    Pthread_mutex_init(&ckp_lst_mtx, NULL);
    ckp_lst_ready = 1;
}

int bdb_checkpoint_list_push(DB_LSN lsn, DB_LSN ckp_lsn, int32_t timestamp)
{
    struct checkpoint_list *ckp = NULL;
    if (!ckp_lst_ready)
        return 0;
    ckp = malloc(sizeof(struct checkpoint_list));
    if (!ckp)
        return ENOMEM;
    ckp->lsn = lsn;
    ckp->ckp_lsn = ckp_lsn;
    ckp->timestamp = timestamp;
    Pthread_mutex_lock(&ckp_lst_mtx);
    listc_abl(&ckp_lst, ckp);
    Pthread_mutex_unlock(&ckp_lst_mtx);

    return 0;
}

static int bdb_checkpoint_list_ok_to_delete_log(int min_keep_logs_age,
                                                int filenum)
{
    struct checkpoint_list *ckp = NULL;
    if (!ckp_lst_ready)
        return 1;
    Pthread_mutex_lock(&ckp_lst_mtx);
    LISTC_FOR_EACH(&ckp_lst, ckp, lnk)
    {
        /* find the first checkpoint which references a file that's larger than
         * the deleted logfile */
        if (ckp->ckp_lsn.file > filenum) {
            /* the furthest point we can recover to is less than what we
             * guaranteed the users*/
            if (time(NULL) - ckp->timestamp < min_keep_logs_age) {
                Pthread_mutex_unlock(&ckp_lst_mtx);
                return 0;
            } else {
                Pthread_mutex_unlock(&ckp_lst_mtx);
                return 1;
            }
        }
    }
    Pthread_mutex_unlock(&ckp_lst_mtx);
    return 1;
}

void bdb_delete_logfile_pglogs(bdb_state_type *bdb_state, int filenum,
                               int flags);
void bdb_delete_timestamp_lsn(bdb_state_type *bdb_state, int32_t timestamp);
extern pthread_mutex_t bdb_gbl_recoverable_lsn_mutex;
extern DB_LSN bdb_gbl_recoverable_lsn;
extern int32_t bdb_gbl_recoverable_timestamp;

void set_repinfo_master_host(bdb_state_type *bdb_state, char *master,
                             const char *func, uint32_t line)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->attr->set_repinfo_master_trace) {
        logmsg(LOGMSG_USER, "Setting repinfo master to %s from %s line %u\n",
               master, func, line);
    }
    bdb_state->repinfo->master_host = master;

    if (master == db_eid_invalid) {
        /* whoismaster_rtn (new_master_callback) will be called when master is available */
        thedb_set_master(db_eid_invalid);
    }
}

static void bdb_checkpoint_list_delete_log(int filenum)
{
    struct checkpoint_list *ckp = NULL;
    if (!ckp_lst_ready)
        return;

    Pthread_mutex_lock(&ckp_lst_mtx);
    while ((ckp = listc_rtl(&ckp_lst)) != NULL) {
        if (ckp->ckp_lsn.file <= filenum) {
            free(ckp);
        } else {
            /* push back to the front of the list */
            listc_atl(&ckp_lst, ckp);
            break;
        }
    }
    /* update bdb_gbl_recoverable_lsn*/
    assert(ckp != NULL);
    bdb_gbl_recoverable_lsn = ckp->lsn;
    bdb_gbl_recoverable_timestamp = ckp->timestamp;

    Pthread_mutex_unlock(&ckp_lst_mtx);
}

static void bdb_snapshot_asof_delete_log(bdb_state_type *bdb_state, int filenum,
                                         time_t timestamp)
{
    bdb_checkpoint_list_delete_log(filenum);
    bdb_delete_logfile_pglogs(bdb_state, filenum, 0);
    bdb_delete_timestamp_lsn(bdb_state, timestamp);
}

void bdb_checkpoint_list_get_ckplsn_before_lsn(DB_LSN lsn, DB_LSN *lsnout)
{
    struct checkpoint_list *ckp = NULL;
    if (!ckp_lst_ready)
        return;
    Pthread_mutex_lock(&ckp_lst_mtx);
    LISTC_FOR_EACH_REVERSE(&ckp_lst, ckp, lnk)
    {
        if (log_compare(&ckp->lsn, &lsn) <= 0) {
            *lsnout = ckp->ckp_lsn;
            Pthread_mutex_unlock(&ckp_lst_mtx);
            return;
        }
    }
    Pthread_mutex_unlock(&ckp_lst_mtx);

    /* huh?? not found? BUG BUG */
    abort();
}

void bdb_checkpoint_list_get_ckp_before_timestamp(int timestamp, DB_LSN *lsnout)
{
    struct checkpoint_list *ckp = NULL;
    if (!ckp_lst_ready)
        return;
    Pthread_mutex_lock(&ckp_lst_mtx);
    LISTC_FOR_EACH_REVERSE(&ckp_lst, ckp, lnk)
    {
        if (ckp->timestamp < timestamp) {
            *lsnout = ckp->lsn;
            Pthread_mutex_unlock(&ckp_lst_mtx);
            return;
        }
    }
    Pthread_mutex_unlock(&ckp_lst_mtx);
}

static void set_some_flags(bdb_state_type *bdb_state, DB *dbp, char *name)
{
    if (bdb_state->attr->checksums) {
        if (dbp->set_flags(dbp, DB_CHKSUM) != 0) {
            logmsg(LOGMSG_ERROR, "error enabling checksums\n");
        }
    }
    if (bdb_state->dbenv->crypto_handle) {
        print(bdb_state, "enabling crypto for %s\n", name);
        if (dbp->set_flags(dbp, DB_ENCRYPT) != 0) {
            logmsg(LOGMSG_ERROR, "error enabling crypto\n");
        }
    }
    if (bdb_state->attr->little_endian_btrees)
        dbp->set_lorder(dbp, 1234 /*little endian*/);
    else
        dbp->set_lorder(dbp, 4321 /*big  endian*/);
}

void bdb_set_recovery(bdb_state_type *bdb_state)
{
    berkdb_set_recovery(bdb_state->dbenv);
}

/* Given a dtanum (0==main record, 1+==blobs) say if it is striped or not */
static int is_datafile_striped(bdb_state_type *bdb_state, int dtanum)
{
    if (bdb_state->bdbtype != BDBTYPE_TABLE) {
        return 0;
    } else if (bdb_state->attr->dtastripe > 0) {
        if (0 == dtanum)
            return 1;
        else if (bdb_state->attr->blobstripe > 0)
            return 1;
        else
            return 0;
    } else {
        return 0;
    }
}

int bdb_get_datafile_num_files(bdb_state_type *bdb_state, int dtanum)
{
    if (is_datafile_striped(bdb_state, dtanum))
        return bdb_state->attr->dtastripe;
    else
        return 1;
}

/* copies the new prefix that is to be used to the provided buffer
 * this prefix is used for temporary tables during schema change, the bdb layer
 * needs to pick it so that it can be unprepended when a temp table gets
 * upgraded to an actual table */
int bdb_get_new_prefix(char *buf, size_t buflen, int *bdberr)
{
    if (!buf || !bdberr) {
        logmsg(LOGMSG_ERROR, "bdb_get_new_prefix: NULL argument\n");
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (sizeof(NEW_PREFIX) - 1 >= buflen) {
        *bdberr = BDBERR_BUFSMALL;
        return -1;
    }

    strncpy0(buf, NEW_PREFIX, buflen);

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/* remove the prefix new. from the beginning of a string (usually a tablename).
 * if the string didn't start with new. then the origional string is
 * returned and bdberr is set to BDBERR_BADARGS
 * NOTE: the returned pointer will point to either the begining or some internal
 * part of the parameter string */
const char *bdb_unprepend_new_prefix(const char *tablename, int *bdberr)
{
    /* if the input didn't start with new. report it by setting bdberr, note
     * that this may not be an error */
    if (strncmp(tablename, NEW_PREFIX, sizeof(NEW_PREFIX) - 1)) {
        *bdberr = BDBERR_BADARGS;
        return tablename;
    }

    *bdberr = BDBERR_NOERROR;
    /* we want to remove the new. prefix from the tablename */
    return tablename + sizeof(NEW_PREFIX) - 1;
}

/* this removes a new.SOMETHING. prefix from a tables name (if it exists)
 * no files are renamed */
void bdb_remove_prefix(bdb_state_type *bdb_state)
{
    if (bdb_state->origname) {
        free(bdb_state->name);
        bdb_state->name = strdup(bdb_state->origname);
        return;
    }

    int bdberr;
    const char *new_name = bdb_unprepend_new_prefix(bdb_state->name, &bdberr);

    if (bdberr == BDBERR_NOERROR) {
        char *old_name = bdb_state->name;
        bdb_state->name = strdup(new_name);
        free(old_name); /* must do this after new name is duped bc new name
                         * points inside old_name */
    }
}

struct bdb_file_version_num_type {
    unsigned long long version_num;
};

enum { BDB_FILE_VERSION_NUM_TYPE_LEN = 8 };

BB_COMPILE_TIME_ASSERT(bdb_file_version_num_type,
                       sizeof(struct bdb_file_version_num_type) ==
                           BDB_FILE_VERSION_NUM_TYPE_LEN);

static uint8_t *bdb_file_version_num_put(
    const struct bdb_file_version_num_type *p_file_version_num_type,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        BDB_FILE_VERSION_NUM_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_file_version_num_type->version_num),
                sizeof(p_file_version_num_type->version_num), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *bdb_file_version_num_get(
    struct bdb_file_version_num_type *p_file_version_num_type,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        BDB_FILE_VERSION_NUM_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_file_version_num_type->version_num),
                sizeof(p_file_version_num_type->version_num), p_buf, p_buf_end);

    return p_buf;
}

const char *bdb_get_tmpdir(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    return bdb_state->tmpdir;
}

/* takes information about a file and constructs its filename, returns bytes
 * written not including NUL */
static int form_file_name_ex(
    bdb_state_type *bdb_state,      /* db to open */
    int is_data_file,               /* see FILE_VERSIONS_FILE_TYPE_* */
    int file_num,                   /* ixnum or dtanum */
    int add_prefix,                 /* whether to add XXX. or directory to
                                     * the front of the filename */
    int isstriped,                  /* whether this dta is striped (ignored
                                     * for index files*/
    int stripenum,                  /* only used if isstriped is true
                                     * (ignored for index files*/
    unsigned long long version_num, /* if == 0 old style names are used
                                     * otherwise new style */
    char *outbuf,                   /* filename is returned here */
    size_t buflen)
{
    int offset, bdberr;
    /*we want to remove the new.SOMETHING. prefix if it's there*/
    const char *tablename;
    if (bdb_state->origname) {
        tablename = bdb_state->origname;
    } else {
        tablename = bdb_unprepend_new_prefix(bdb_state->name, &bdberr);
    }
    size_t orig_buflen = buflen;

    /* start building the file name */
    if (add_prefix) {
        offset = snprintf(outbuf, buflen, "XXX.");
        outbuf += offset;
        buflen -= offset;
    }
    offset = snprintf(outbuf, buflen, "%s", tablename);
    outbuf += offset;
    buflen -= offset;

    /*append the version number for this table db*/
    if (version_num) {
        unsigned long long pr_vers;
        uint8_t *p_buf, *p_buf_end;
        struct bdb_file_version_num_type p_file_version_num_type;

        char *file_ext = "index";
        if (is_data_file) {
            if (file_num)
                file_ext = "blob";
            else
                file_ext = "data";
        }

        p_file_version_num_type.version_num = version_num;
        p_buf = (uint8_t *)&pr_vers;
        p_buf_end = p_buf + sizeof(pr_vers);
        bdb_file_version_num_put(&(p_file_version_num_type), p_buf, p_buf_end);

        offset = snprintf(outbuf, buflen, "_%016llx.%s", pr_vers, file_ext);
        outbuf += offset;
        buflen -= offset;
    } else /*if we have no version number*/
    {
        char *file_ext = "ix";
        if (is_data_file)
            file_ext = "dta";

        /*add the tablename and the suffix*/
        offset = snprintf(outbuf, buflen, ".%s", file_ext);
        outbuf += offset;
        buflen -= offset;

        /* if this is an index or a blob blob, add its number */
        if (file_num > 0 || !is_data_file) {
            offset = snprintf(outbuf, buflen, "%d", file_num);
            outbuf += offset;
            buflen -= offset;
        }
    }

    /*if datafile and striped add the stripe number*/
    if (is_data_file && isstriped) {
        offset = snprintf(outbuf, buflen, "s%d", stripenum);
        outbuf += offset;
        buflen -= offset;
    }

    return orig_buflen - buflen;
}

int bdb_form_file_name(bdb_state_type *bdb_state, int is_data_file, int filenum,
                       int stripenum, unsigned long long version_num,
                       char *outbuf, size_t buflen)
{
    int isstriped = 0;

    if (is_data_file) {
        if (filenum == 0) {
            /* data */
            if (bdb_state->bdbtype == BDBTYPE_TABLE &&
                bdb_state->attr->dtastripe > 0)
                isstriped = 1;
        } else {
            /* blob */
            if (bdb_state->attr->blobstripe > 0)
                isstriped = 1;
        }
    }

    return form_file_name_ex(bdb_state, is_data_file, filenum, 0, isstriped,
                             stripenum, version_num, outbuf, buflen);
}

/*figures out what the version number is (if any) and calls form_file_name_ex*/
static int form_file_name(bdb_state_type *bdb_state, DB_TXN *tid,
                          int is_data_file, int file_num, int isstriped,
                          int stripenum, char *outbuf, size_t buflen)
{
    unsigned long long version_num;
    int rc, bdberr;
    tran_type tran = {0};

    tran.tid = tid;

    if (is_data_file)
        rc = bdb_get_file_version_data(bdb_state, &tran, file_num, &version_num,
                                       &bdberr);
    else
        rc = bdb_get_file_version_index(bdb_state, &tran, file_num,
                                        &version_num, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        if (bdberr == BDBERR_DBEMPTY) {
            /*fprintf( stderr, "Not using version numbers (or db is empty) for
             * "*/
            /*"table: %s with type: %d, and num: %d, defaulting to old "*/
            /*"name scheme\n", tablename, is_data_file, file_num );*/
        } else if (bdberr == BDBERR_FETCH_DTA) {
            /*fprintf( stderr, "No version number found for table: %s with type:
             * "*/
            /*"%s, and num: %d, defaulting to old name scheme\n",*/
            /*bdb_state->name, (is_data_file)?"data":"idx", file_num );*/
        } else
            logmsg(LOGMSG_ERROR, 
                    "Version number lookup failed with bdberr: %d for "
                    "table: %s with type: %s, and num: %d, defaulting to old "
                    "name scheme\n",
                    bdberr, bdb_state->name, (is_data_file) ? "data" : "idx",
                    file_num);
    }

    return form_file_name_ex(bdb_state, is_data_file, file_num,
                             1 /*add_prefix*/, isstriped, stripenum,
                             version_num, outbuf, buflen);
}

/* calls form_file_name for a datafile */
static int form_datafile_name(bdb_state_type *bdb_state, DB_TXN *tid,
                              int dtanum, int stripenum, char *outbuf,
                              size_t buflen)
{
    /*find out whether this db is striped*/
    int isstriped = 0;
    if (dtanum > 0) {
        if (bdb_state->attr->blobstripe > 0)
            isstriped = 1;
    } else if (bdb_state->bdbtype == BDBTYPE_TABLE &&
               bdb_state->attr->dtastripe > 0)
        isstriped = 1;

    return form_file_name(bdb_state, tid, 1 /*is_data_file*/, dtanum, isstriped,
                          stripenum, outbuf, buflen);
}

/* calls form_file_name for an indexfile */
static int form_indexfile_name(bdb_state_type *bdb_state, DB_TXN *tid,
                               int ixnum, char *outbuf, size_t buflen)
{
    return form_file_name(bdb_state, tid, 0 /*is_data_file*/, ixnum,
                          0 /*isstriped*/, 0 /*stripenum*/, outbuf, buflen);
}

static int should_stop_looking_for_queuedb_files(bdb_state_type *bdb_state,
                                                 tran_type *tran, int file_num,
                                              unsigned long long *file_version)
{
    unsigned long long local_file_version = 0;
    int bdberr = 0;
    if (bdb_get_file_version_qdb(bdb_state, tran, file_num,
                                 &local_file_version, &bdberr) != 0) {
        /*
        ** NOTE: For queuedb, all files after the first one are optional
        **       and may not actually exist.
        */
        if (file_version != NULL) *file_version = 0;
        if (file_num > 0) {
            logmsg(LOGMSG_DEBUG,
                "%s: queuedb %s file %d version not found, stopping...\n",
                __func__, bdb_state->name, file_num);
            return 1;
        }
    }
    if (file_version != NULL) *file_version = local_file_version;
    return 0;
}

static void form_queuedb_name_int(bdb_state_type *bdb_state, char *name,
                                  size_t len, unsigned long long file_version)
{
    if (file_version != 0) {
        snprintf0(name, len, "XXX.%s_%016llx.queuedb", bdb_state->name,
                  file_version);
    } else {
        snprintf0(name, len, "XXX.%s.queuedb", bdb_state->name);
    }
}

static int form_queuedb_name(bdb_state_type *bdb_state, tran_type *tran,
                             int file_num, int create, char *name, size_t len)
{
    unsigned long long ver;
    int rc, bdberr;
    if (create && USE_GENID_IN_QUEUEDB_FILE_NAME()) {
        ver = flibc_htonll(bdb_get_cmp_context(bdb_state));
        rc = bdb_new_file_version_qdb(bdb_state, tran, file_num, ver, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            return -1;
        }
    }
    /* NOTE: This point is reached even if we (just) successfully
    **       set the file version above. */
    if (bdb_get_file_version_qdb(bdb_state, tran, file_num, &ver,
                                 &bdberr) == 0) {
        /* success, do nothing yet. */
    } else {
        /* no version -AND- do fallback to versionless */
        ver = 0;
    }
    form_queuedb_name_int(bdb_state, name, len, ver);
    return 0;
}

int bdb_bulk_import_copy_cmd_add_tmpdir_filenames(
    bdb_state_type *bdb_state, unsigned long long src_data_genid,
    unsigned long long dst_data_genid,
    const unsigned long long *p_src_index_genids,
    const unsigned long long *p_dst_index_genids, size_t num_index_genids,
    const unsigned long long *p_src_blob_genids,
    const unsigned long long *p_dst_blob_genids, size_t num_blob_genids,
    char *outbuf, size_t buflen, int *bdberr)
{
    int dtanum;
    int ixnum;
    int len;
    int offset = 0;

    if (!bdb_state || !p_src_index_genids || !p_dst_index_genids ||
        !p_src_blob_genids || !p_dst_blob_genids || !outbuf || !bdberr ||
        bdb_state->numdtafiles - 1 /*1st data file isn't blob*/
            != num_blob_genids ||
        bdb_state->numix != num_index_genids) {
        logmsg(LOGMSG_ERROR, "%s: null or invalid argument\n", __func__);

        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* add -dsttmpdir */
    len = snprintf(outbuf + offset, buflen - offset, " -dsttmpdir %s",
                   bdb_state->parent->tmpdir);
    offset += len;
    if (len < 0 || offset >= buflen) {
        logmsg(LOGMSG_ERROR, 
                "%s: adding -dsttmpdir arg failed or string was too "
                "long for buffer this string len: %d total string len: %d\n",
                __func__, len, offset);
        *bdberr = BDBERR_BUFSMALL;
        return -1;
    }

    /* add data files */
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        int strnum;
        int num_stripes = bdb_get_datafile_num_files(bdb_state, dtanum);
        int isstriped = 0;
        unsigned long long src_version_num;
        unsigned long long dst_version_num;

        /*find out if this datafile is striped and get the right version num*/
        if (dtanum > 0) {
            src_version_num = p_src_blob_genids[dtanum - 1];
            dst_version_num = p_dst_blob_genids[dtanum - 1];
            if (bdb_state->attr->blobstripe > 0)
                isstriped = 1;
        } else {
            src_version_num = src_data_genid;
            dst_version_num = dst_data_genid;
            if (bdb_state->bdbtype == BDBTYPE_TABLE &&
                bdb_state->attr->dtastripe > 0)
                isstriped = 1;
        }

        /* for each stripe add a -file param */
        for (strnum = 0; strnum < num_stripes; ++strnum) {
            /* add -file */
            len = snprintf(outbuf + offset, buflen - offset, " -file ");
            offset += len;
            if (len < 0 || offset >= buflen) {
                logmsg(LOGMSG_ERROR, 
                        "%s: adding data -file arg failed or string was"
                        " too long for buffer this string len: %d total string "
                        "len: %d\n",
                        __func__, len, offset);
                *bdberr = BDBERR_BUFSMALL;
                return -1;
            }

            /* add src filename */
            len = form_file_name_ex(bdb_state, 1 /*is_data_file*/, dtanum,
                                    0 /*add_prefix*/, isstriped, strnum,
                                    src_version_num, outbuf + offset,
                                    buflen - offset);
            offset += len + 1 /*include the space we're about to add*/;
            if (len < 0 || offset >= buflen) {
                logmsg(LOGMSG_ERROR, 
                        "%s: adding src data filename failed or string "
                        "was too long for buffer this string len: %d total "
                        "string len: %d\n",
                        __func__, len, offset);
                *bdberr = BDBERR_BUFSMALL;
                return -1;
            }

            /* add a space. this removes the NUL, it will be re added by the
             * form_file_name_ex() below */
            outbuf[offset - 1] = ' ';

            /* add dst filename */
            len = form_file_name_ex(bdb_state, 1 /*is_data_file*/, dtanum,
                                    0 /*add_prefix*/, isstriped, strnum,
                                    dst_version_num, outbuf + offset,
                                    buflen - offset);
            offset += len;
            if (len < 0 || offset >= buflen) {
                logmsg(LOGMSG_ERROR, 
                        "%s: adding dst data filename failed or string "
                        "was too long for buffer this string len: %d total "
                        "string len: %d\n",
                        __func__, len, offset);
                *bdberr = BDBERR_BUFSMALL;
                return -1;
            }
        }
    }

    /* for each index add a -file param */
    for (ixnum = 0; ixnum < bdb_state->numix; ixnum++) {
        /* add -file */
        len = snprintf(outbuf + offset, buflen - offset, " -file ");
        offset += len;
        if (len < 0 || offset >= buflen) {
            logmsg(LOGMSG_ERROR, 
                    "%s: adding index -file arg failed or string was "
                   "too long for buffer this string len: %d total string len: "
                   "%d\n",
                    __func__, len, offset);
            *bdberr = BDBERR_BUFSMALL;
            return -1;
        }

        /* add src filename */
        len = form_file_name_ex(bdb_state, 0 /*is_data_file*/, ixnum,
                                0 /*add_prefix*/, 0 /*isstriped*/, 0 /*strnum*/,
                                p_src_index_genids[ixnum], outbuf + offset,
                                buflen - offset);
        offset += len + 1 /*include the space we're about to add*/;
        if (len < 0 || offset >= buflen) {
            logmsg(LOGMSG_ERROR, "%s: adding src index filename failed or string "
                    "was too long for buffer this string len: %d total string "
                    "len: %d\n",
                    __func__, len, offset);
            *bdberr = BDBERR_BUFSMALL;
            return -1;
        }

        /* add a space. this removes the NUL, it will be re added by the
         * form_file_name_ex() below */
        outbuf[offset - 1] = ' ';

        /* add dst filename */
        len = form_file_name_ex(bdb_state, 0 /*is_data_file*/, ixnum,
                                0 /*add_prefix*/, 0 /*isstriped*/, 0 /*strnum*/,
                                p_dst_index_genids[ixnum], outbuf + offset,
                                buflen - offset);
        offset += len;
        if (len < 0 || offset >= buflen) {
            logmsg(LOGMSG_ERROR, "%s: adding dst index filename failed or string "
                   "was too long for buffer this string len: %d total string "
                   "len: %d\n",
                   __func__, len, offset);
            *bdberr = BDBERR_BUFSMALL;
            return -1;
        }
    }

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/*moves an entire table over to the new versioning database*/
int bdb_rename_file_versioning_table(bdb_state_type *bdb_state,
                                     tran_type *input_trans, char *newtblname,
                                     int *bdberr)
{
    int dtanum, ixnum, retries = 0;
    unsigned long long version_num;
    unsigned long long new_version_num;
    char oldname[80], newname[80];
    tran_type *tran;
    char *orig_name;

    orig_name = bdb_state->name;

retry:
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__,
               retries);
        return -1;
    }

    /*if the user didn't give us a transaction, create our own*/
    if (!input_trans) {
        tran = bdb_tran_begin(bdb_state, NULL, bdberr);
        if (!tran) {
            if (*bdberr == BDBERR_DEADLOCK)
                goto retry;

            logmsg(LOGMSG_ERROR, "%s: failed to get transaction\n", __func__);
            return -1;
        }
    } else
        tran = input_trans;

    /* set table version num, this is used to test if the table is using file
     * versions */
    bdb_state->name = newtblname;
    new_version_num = bdb_get_cmp_context(bdb_state);
    if (bdb_new_file_version_table(bdb_state, tran, new_version_num, bdberr) ||
        *bdberr != BDBERR_NOERROR)
        goto backout;

    /* get existing name version */
    bdb_state->name = orig_name;
    if (bdb_get_file_version_table(bdb_state, tran, &version_num, bdberr) &&
        *bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "failed to retrieve existing table version\n");
        *bdberr = BDBERR_MISC;
        goto backout;
    }

    /* update data files */
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        int strnum, num_stripes = bdb_get_datafile_num_files(bdb_state, dtanum),
                    isstriped = 0;

        new_version_num = bdb_get_cmp_context(bdb_state);

        /* get data versioning existing table */
        if (bdb_get_file_version_data(bdb_state, tran, dtanum, &version_num,
                                      bdberr) &&
            *bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "failed to retrieve existing index version\n");
            *bdberr = BDBERR_MISC;
            goto backout;
        }

        /*find out if this datafile is striped*/
        if (dtanum > 0) {
            if (bdb_state->attr->blobstripe > 0)
                isstriped = 1;
        } else if (bdb_state->bdbtype == BDBTYPE_TABLE &&
                   bdb_state->attr->dtastripe > 0)
            isstriped = 1;

        /*save all of the stripe's names*/
        for (strnum = 0; strnum < num_stripes; ++strnum) {
            /*get the old filename*/
            form_file_name_ex(bdb_state, 1 /*is_data_file*/, dtanum,
                              1 /*add_prefix*/, isstriped, strnum, version_num,
                              oldname, sizeof(oldname));
            /*get the new filename*/
            bdb_state->name = newtblname;
            form_file_name_ex(bdb_state, 1 /*is_data_file*/, dtanum,
                              1 /*add_prefix*/, isstriped, strnum,
                              new_version_num, newname, sizeof(newname));
            bdb_state->name = orig_name;

            /*rename the file*/
            logmsg(LOGMSG_INFO, "%s: renaming %s to %s\n", __func__, oldname,
                   newname);
            if (bdb_rename_file(bdb_state, tran->tid, oldname, newname,
                                bdberr) ||
                *bdberr != BDBERR_NOERROR)
                goto backout;
        }

        /*update the file's version*/
        bdb_state->name = newtblname;
        if (bdb_new_file_version_data(bdb_state, tran, dtanum, new_version_num,
                                      bdberr) ||
            *bdberr != BDBERR_NOERROR) {
            bdb_state->name = orig_name;
            goto backout;
        }
        bdb_state->name = orig_name;
    }

    /* update the index files */
    for (ixnum = 0; ixnum < bdb_state->numix; ixnum++) {
        new_version_num = bdb_get_cmp_context(bdb_state);

        /* get index versioning existing table */
        if (bdb_get_file_version_index(bdb_state, tran, ixnum, &version_num,
                                       bdberr) &&
            *bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "failed to retrieve existing index version\n");
            *bdberr = BDBERR_MISC;
            goto backout;
        }

        /*get old name*/
        form_file_name_ex(bdb_state, 0 /*is_data_file*/, ixnum,
                          1 /*add_prefix*/, 0 /*isstriped*/, 0 /*strnum*/,
                          version_num, oldname, sizeof(oldname));
        /*get new name*/
        bdb_state->name = newtblname;
        form_file_name_ex(bdb_state, 0 /*is_data_file*/, ixnum,
                          1 /*add_prefix*/, 0 /*isstriped*/, 0 /*strnum*/,
                          new_version_num, newname, sizeof(newname));
        bdb_state->name = orig_name;

        /*rename*/
        logmsg(LOGMSG_INFO, "%s: renaming %s to %s\n", __func__, oldname,
               newname);
        if (bdb_rename_file(bdb_state, tran->tid, oldname, newname, bdberr) ||
            *bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: ERROR converting %s to %s. Check FILE PERMISSIONS\n",
                    __func__, oldname, newname);
            goto backout;
        }

        /*update version*/
        bdb_state->name = newtblname;
        if (bdb_new_file_version_index(bdb_state, tran, ixnum, new_version_num,
                                       bdberr) ||
            *bdberr != BDBERR_NOERROR) {
            bdb_state->name = orig_name;
            goto backout;
        }
        bdb_state->name = orig_name;
    }

    /*commit if we created our own transaction*/
    if (!input_trans) {
        if (bdb_tran_commit(bdb_state, tran, bdberr) &&
            *bdberr != BDBERR_NOERROR)
            goto backout;
    }

    *bdberr = BDBERR_NOERROR;
    return 0; /*success*/

backout:
    /*if we created the transaction*/
    if (!input_trans) {
        int prev_bdberr = *bdberr;

        /*kill the transaction*/

        if (bdb_tran_abort(bdb_state, tran, bdberr) && !BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: trans abort failed with bdberr %d\n",
                   __func__, *bdberr);
            return -1;
        }

        *bdberr = prev_bdberr;
        if (*bdberr == BDBERR_DEADLOCK)
            goto retry;

        logmsg(LOGMSG_ERROR, "%s: failed with bdberr %d\n", __func__, *bdberr);
    }
    return -1;
}

int bdb_rename_table(bdb_state_type *bdb_state, tran_type *tran, char *newname,
                     int *bdberr)
{
    DB_TXN *tid = tran ? tran->tid : NULL;
    char *saved_name;
    char *saved_origname; /* certain sc set this, preserve */
    int rc;

    __dbreg_lock_lazy_id(bdb_state->dbenv);

    rc = close_dbs_flush(bdb_state);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "upgrade: open_dbs as master failed\n");
        __dbreg_unlock_lazy_id(bdb_state->dbenv);
        return -1;
    }

    saved_origname = bdb_state->origname;
    bdb_state->origname = NULL;
    rc = bdb_rename_file_versioning_table(bdb_state, tran, newname, bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "upgrade: open_dbs as master failed\n");
        bdb_state->origname = saved_origname;
        __dbreg_unlock_lazy_id(bdb_state->dbenv);
        return -1;
    }

    saved_name = bdb_state->name;
    bdb_state->name = newname;
    rc = open_dbs(bdb_state, 1, 1, 0, tid, 0);
    if (rc != 0) {
        bdb_state->name = saved_name;
        bdb_state->origname = saved_origname;
        logmsg(LOGMSG_ERROR, "upgrade: open_dbs as master failed\n");
        __dbreg_unlock_lazy_id(bdb_state->dbenv);
        return -1;
    }
    bdb_state->name = saved_name;
    bdb_state->isopen = 1;

    __dbreg_unlock_lazy_id(bdb_state->dbenv);
    return 0;
}

struct del_list_item {
    int is_data_file;
    int file_num;
    unsigned long long version_num;
    LINKC_T(struct del_list_item) lnk;
};

/* creates a new list, the list must eventually be passed to bdb_del_list_free
 * to avoid leaking memory */
void *bdb_del_list_new(int *bdberr)
{
    listc_t *list = listc_new(offsetof(struct del_list_item, lnk));

    if (list)
        *bdberr = BDBERR_NOERROR;
    else
        *bdberr = BDBERR_MISC;

    return list;
}

/* records a files details so that it can be deleted later even if it's version
 * number is changed in the version table */
static int bdb_del_list_add(bdb_state_type *bdb_state, tran_type *tran,
                            void *list, int is_data_file, int file_num,
                            int *bdberr)
{
    unsigned long long version_num;
    struct del_list_item *item;
    int rc;

    if (!bdb_state || !list || !bdberr) {
        logmsg(LOGMSG_ERROR, "bdb_del_list_add: null or invalid argument\n");
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /*get the version num*/
    if (is_data_file)
        rc = bdb_get_file_version_data(bdb_state, tran, file_num, &version_num,
                                       bdberr);
    else
        rc = bdb_get_file_version_index(bdb_state, tran, file_num, &version_num,
                                        bdberr);
    if (rc || *bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "bdb_del_list_add: failed to look up version num for "
                        "file\n");
        return -1;
    }

    /*create a new item*/
    item = malloc(sizeof(*item));
    if (!item) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /*populate the item*/
    item->is_data_file = is_data_file;
    item->file_num = file_num;
    item->version_num = version_num;

    /*add it to the list*/
    listc_atl(list, item);

    /*
    fprintf(stderr, "adding %016llx %d %d to be deleted\n",
       item->version_num, item->is_data_file, item->file_num);
    */

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/*calls bdb_del_list_add for a datafile*/
int bdb_del_list_add_data(bdb_state_type *bdb_state, tran_type *tran,
                          void *list, int dtanum, int *bdberr)
{
    if (!bdb_state || !list || !bdberr) {
        logmsg(LOGMSG_ERROR, "bdb_del_list_add_data: null or invalid argument\n");
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    return bdb_del_list_add(bdb_state, tran, list, 1 /*is_data_file*/, dtanum,
                            bdberr);
}

/*calls bdb_del_list_add for a indexfile*/
int bdb_del_list_add_index(bdb_state_type *bdb_state, tran_type *tran,
                           void *list, int ixnum, int *bdberr)
{
    if (!bdb_state || !list || !bdberr) {
        logmsg(LOGMSG_ERROR, "bdb_del_list_add_index: null or invalid argument\n");
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    return bdb_del_list_add(bdb_state, tran, list, 0 /*is_data_file*/, ixnum,
                            bdberr);
}

/*calls bdb_del_list_add for all files in database*/
int bdb_del_list_add_all(bdb_state_type *bdb_state, tran_type *tran, void *list,
                         int *bdberr)
{
    int dtanum, ixnum;

    if (!bdb_state || !list || !bdberr) {
        logmsg(LOGMSG_ERROR, "bdb_del_list_add_all: null or invalid argument\n");
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* update data files */
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++)
        if (bdb_del_list_add_data(bdb_state, tran, list, dtanum, bdberr))
            return -1;

    /* update the index files */
    for (ixnum = 0; ixnum < bdb_state->numix; ixnum++)
        if (bdb_del_list_add_index(bdb_state, tran, list, ixnum, bdberr))
            return -1;

    return 0;
}

int bdb_del_list_free(void *list, int *bdberr)
{
    struct del_list_item *item, *tmp;
    listc_t *list_ptr = list;

    /* free each item */
    LISTC_FOR_EACH_SAFE(list_ptr, item, tmp, lnk) {
        /* remove and free item */
        free(listc_rfl(list, item));
    }

    listc_free(list);

    *bdberr = BDBERR_NOERROR;
    return 0;
}

/*

 XXX.<dbname>.<rest>                    -> <bdb_state->dir>/<dbname>.<rest>

  infile                                     outfile
----------                                ----------------
 XXX.<dbname>.dta                       -> /bb/data/alexdb.dta
 XXX.<dbname>.ix0                       -> /bb/data/alexdb.ix0
 XXX.<dbname>.ix1                       -> /bb/data/alexdb.ix1
 XXX.<dbname>.ix245                     -> /bb/data/alexdb.ix245
 XXX.<dbname>.txn/./log.0000000003      -> /bb/data/alexdb.txn/./log.0000000003
 XXX.<dbname>.txn/./__dbq.XXX.myq.queue -> /bb/data/alexdb.txn/./__dbq.myq.queue
 /bb/data/alexdb.txn/./__dbq.XXX.myq.queue ->
/bb/data/alexdb.txn/./__dbq.myq.queue

 (In some circumstances, the input file does not begin with XXX. put still
 requires translation.  This really only happens if we opned the environment
 with an absolute path, as we would have done for comdb2_db_recover.  So even
 though this doesn't happen in regular comdb2, let's be robust against it
 anyway.)

*/

bdb_state_type *gbl_bdb_state;

char *bdb_trans(const char infile[], char outfile[])
{
    bdb_state_type *bdb_state;
    char *p;
    int len;

    if (!infile) {
        if (outfile)
            outfile[0] = '\0';
        return NULL;
    }

    /*bdb_state = pthread_getspecific(bdb_key);*/
    bdb_state = gbl_bdb_state;

    if (bdb_state == NULL) {
        strcpy(outfile, infile);
        return outfile;
    }

    /* Copy to outfile.  If leading with a XXX., strip this off and replace with
     * full path. */
    if (strncmp(infile, "XXX.", 4) == 0) {
        sprintf(outfile, "%s/%s", bdb_state->dir, infile + 4);
    } else {
        strcpy(outfile, infile);
    }

    /* Look for queue extents and correct them. */
    p = strstr(outfile, "/__dbq.XXX.");
    if (p) {
        p += 7;
        len = strlen(p);
        memmove(p, p + 4, len - 4 + 1 /* copy \0 byte too! */);
    }

    return outfile;
}

int net_hostdown_rtn(netinfo_type *netinfo_ptr, char *host);
int net_newnode_rtn(netinfo_type *netinfo_ptr, char *hostname, int portnum);
int net_cmplsn_rtn(netinfo_type *netinfo_ptr, void *x, int xlen, void *y,
                   int ylen);
int net_getlsn_rtn(netinfo_type *netinfo_ptr, void *record, int len, int *file,
                   int *offset);

static void net_startthread_rtn(void *arg)
{
    bdb_thread_event((bdb_state_type *)arg, 1);
}

static void net_stopthread_rtn(void *arg)
{
    bdb_thread_event((bdb_state_type *)arg, 0);
}

/* According to the berkdb docs, after the DB/DBENV close() functions have
 * been called the handle can no longer be used regardless of the outcome.
 * Hence this function will now never fail - although it may spit out errors.
 * After this is called, the db is closed.
 */
static int close_dbs_int(bdb_state_type *bdb_state, DB_TXN *tid, int flags)
{
    int rc;
    int i;
    int dtanum, strnum;
    u_int8_t fileid[21] = {0};
    char fid_str[41] = {0};

    print(bdb_state, "in %s(name=%s)\n", __func__, bdb_state->name);

    if (!bdb_state->isopen) {
        print(bdb_state, "%s not open, not closing\n", bdb_state->name);
        logmsg(LOGMSG_DEBUG, "%s:%d %s not open, not closing\n", __func__,
               __LINE__, bdb_state->name);
        return 0;
    }

    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB) {
        if (!bdb_trigger_ispaused(bdb_state)) {
            bdb_trigger_close(bdb_state);
        }
    }

    for (dtanum = 0; dtanum < MAXDTAFILES; dtanum++) {
        for (strnum = 0; strnum < MAXDTASTRIPE; strnum++) {
            if (bdb_state->dbp_data[dtanum][strnum]) {
                bdb_state->dbp_data[dtanum][strnum]->get_fileid(
                    bdb_state->dbp_data[dtanum][strnum], fileid);
                fileid_str(fileid, fid_str);
                logmsg(LOGMSG_DEBUG, "%s:%d  closing fileid %s\n", __func__,
                       __LINE__, fid_str);
                rc = bdb_state->dbp_data[dtanum][strnum]->closetxn(
                    bdb_state->dbp_data[dtanum][strnum], tid, flags);
                if (0 != rc) {
                    logmsg(LOGMSG_ERROR,
                           "%s: error closing %s[%d][%d]: %d %s\n", __func__,
                           bdb_state->name, dtanum, strnum, rc,
                           db_strerror(rc));
                }
            } else if (notclosingdta_trace) {
                logmsg(LOGMSG_DEBUG,
                       "%s:%d not closing dtafile %d stripe %d "
                       "(NULL ptr)\n",
                       __func__, __LINE__, dtanum, strnum);
            }
        }
    }

    if (bdb_state->bdbtype == BDBTYPE_TABLE) {
        logmsg(LOGMSG_DEBUG, "%s:%d  looking through table %s numix %d\n",
               __func__, __LINE__, bdb_state->name, bdb_state->numix);
        for (i = 0; i < bdb_state->numix; i++) {
            /*fprintf(stderr, "closing ix %d\n", i);*/
            bdb_state->dbp_ix[i]->get_fileid(bdb_state->dbp_ix[i], fileid);
            fileid_str(fileid, fid_str);
            logmsg(LOGMSG_DEBUG, "%s:%d closing fileid %s\n", __func__,
                   __LINE__, fid_str);
            rc = bdb_state->dbp_ix[i]->closetxn(bdb_state->dbp_ix[i], tid,
                                                flags);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: error closing %s->dbp_ix[%d] %d %s\n",
                       __func__, bdb_state->name, i, rc, db_strerror(rc));
            }
        }
    }

    /* get rid of our handles since they're no longer valid - don't want to get
     * fooled by dangling pointers! */
    bzero(bdb_state->dbp_data, sizeof(bdb_state->dbp_data));
    bzero(bdb_state->dbp_ix, sizeof(bdb_state->dbp_ix));

    /* since we always succeed, mark the db as closed now */
    bdb_state->isopen = 0;

    return 0;
}

static int close_dbs(bdb_state_type *bdb_state)
{
    return close_dbs_int(bdb_state, NULL, DB_NOSYNC);
}

static int close_dbs_txn(bdb_state_type *bdb_state, DB_TXN *txn)
{
    return close_dbs_int(bdb_state, txn, DB_NOSYNC);
}

static int close_dbs_flush(bdb_state_type *bdb_state)
{
    return close_dbs_int(bdb_state, NULL, 0);
}

int bdb_isopen(bdb_state_type *bdb_handle) { return bdb_handle->isopen; }

int bdb_flush_up_to_lsn(bdb_state_type *bdb_state, unsigned file,
                        unsigned offset)
{
    int rc;
    DB_LSN lsn;
    lsn.file = file;
    lsn.offset = offset;

    rc = bdb_state->dbenv->log_flush(bdb_state->dbenv, &lsn);
    return rc;
}

static int bdb_flush_cache(bdb_state_type *bdb_state)
{

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    BDB_READLOCK("bdb_flush_cache");

    bdb_state->dbenv->memp_sync(bdb_state->dbenv, NULL);

    BDB_RELLOCK();

    return 0;
}

int bdb_dump_cache_to_file(bdb_state_type *bdb_state, const char *file,
                           int max_pages)
{
    int rc, fd;
    SBUF2 *s;
    if ((fd = open(file, O_WRONLY | O_TRUNC | O_CREAT, 0666)) < 0 ||
        (s = sbuf2open(fd, 0)) == NULL) {
        if (fd >= 0)
            close(fd);
        logmsg(LOGMSG_ERROR, "%s error opening %s: %d\n", __func__, file,
               errno);
        return -1;
    }
    rc = bdb_state->dbenv->memp_dump(bdb_state->dbenv, s, max_pages);
    sbuf2close(s);
    return rc;
}

int bdb_load_cache(bdb_state_type *bdb_state, const char *file)
{
    int rc, fd;
    SBUF2 *s;
    if ((fd = open(file, O_RDONLY, 0)) < 0 || (s = sbuf2open(fd, 0)) == NULL) {
        if (fd >= 0)
            close(fd);
        return -1;
    }
    rc = bdb_state->dbenv->memp_load(bdb_state->dbenv, s);
    sbuf2close(s);
    return rc;
}

int bdb_load_cache_default(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->memp_load_default(bdb_state->dbenv);
}

int bdb_dump_cache_default(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->memp_dump_default(bdb_state->dbenv, 1);
}

static int bdb_flush_int(bdb_state_type *bdb_state, int *bdberr, int force)
{
    int rc;
    int start, end;

    *bdberr = BDBERR_NOERROR;

    rc = bdb_state->dbenv->log_flush(bdb_state->dbenv, NULL);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "log_flush err %d\n", rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost)
        bdb_flush_cache(bdb_state);
    else {
        start = comdb2_time_epochms();
        rc = ll_checkpoint(bdb_state, force);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "txn_checkpoint err %d\n", rc);
            *bdberr = BDBERR_MISC;
            return -1;
        }
        end = comdb2_time_epochms();
        ctrace("checkpoint took %dms\n", end - start);
    }

    return 0;
}

int bdb_flush(bdb_state_type *bdb_state, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_flush");

    rc = bdb_flush_int(bdb_state, bdberr, 1);

    BDB_RELLOCK();

    return rc;
}

int bdb_flush_noforce(bdb_state_type *bdb_state, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_flush");

    rc = bdb_flush_int(bdb_state, bdberr, 0);

    BDB_RELLOCK();

    return rc;
}

static int bdb_lock_children_lock(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    Pthread_mutex_lock(&(bdb_state->children_lock));
    assert(!bdb_state->have_children_lock);
    bdb_state->have_children_lock = 1;
    if (gbl_debug_children_lock) {
        logmsg(LOGMSG_USER, "Acquired children lock\n");
        cheap_stack_trace();
    }
    return 0;
}

static int bdb_unlock_children_lock(bdb_state_type *bdb_state)
{

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    assert(bdb_state->have_children_lock);
    bdb_state->have_children_lock = 0;
    Pthread_mutex_unlock(&(bdb_state->children_lock));
    if (gbl_debug_children_lock) {
        logmsg(LOGMSG_USER, "Released children lock\n");
        cheap_stack_trace();
    }
    return 0;
}

void bdb_prepare_close(bdb_state_type *bdb_state)
{
    netinfo_type *netinfo_ptr = bdb_state->repinfo->netinfo;

    if (is_real_netinfo(netinfo_ptr)) {
        /* get me off the network */
        if (gbl_libevent) {
            stop_event_net();
        } else {
            net_send_decom_all(netinfo_ptr, gbl_myhostname);
            osql_process_message_decom(gbl_myhostname);
            sleep(2);
            net_exiting(netinfo_ptr);
            osql_net_exiting();
        }
    }

    net_cleanup_netinfo(netinfo_ptr);
    osql_cleanup_netinfo();
}

/* this routine is only used to CLOSE THE WHOLE DB (env) */
static int bdb_close_int(bdb_state_type *bdb_state, int envonly)
{
    int rc;
    bdb_state_type *child;
    int i;
    int bdberr;
    int last;
    DB_TXN *tid;
    /* lock everyone out of the bdb code */
    BDB_WRITELOCK(__func__);

    /* force a checkpoint */
    rc = ll_checkpoint(bdb_state, 1);

    /* discard recovered prepares */
    bdb_state->dbenv->txn_discard_all_recovered(bdb_state->dbenv);

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bdb_lock_children_lock(bdb_state);
    for (i = 0; i < bdb_state->numchildren; i++) {
        child = bdb_state->children[i];
        if (child) {
            child->exiting = 1;
        }
    }
    bdb_unlock_children_lock(bdb_state);

#   if 0
    /* Wait for ongoing election to abort. */
    while (1) {
        Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));
        int in_election = bdb_state->repinfo->in_election;
        Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));
        if (!in_election)
            break;
        logmsg(LOGMSG_WARN, "%s: Election in progress.\n", __func__);
        sleep(1);
    }
#   endif

    bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);

    /* close all database files.   doesn't fail. */
    if (!envonly) {
        rc = close_dbs(bdb_state);
    }

    /* now do it for all of our children */
    bdb_lock_children_lock(bdb_state);
    for (i = 0; i < bdb_state->numchildren; i++) {
        child = bdb_state->children[i];

        /* close all of our databases.  doesn't fail. */
        if (child) {
            rc = close_dbs(child);
            bdb_access_destroy(child);
        }
    }
    bdb_unlock_children_lock(bdb_state);

    /* Commit */
    rc = tid->commit(tid, 0);
    assert(rc == 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_close_int: commit %d\n", rc);
    }

    /* close our transactional environment.  note that according to berkdb
     * docs the handle is invalid after this is called regardless of the
     * outcome, therefore there is no concept of failure here. */
    rc = bdb_state->dbenv->close(bdb_state->dbenv, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_close_int: error closing env %d %s\n", rc,
                db_strerror(rc));
    }
    bdb_state->dbenv = NULL;

/* free our dynamically allocated memory */
#ifdef NOTYET
    Pthread_attr_destroy(&(bdb_state->pthread_attr_detach));
#endif

    /* clear temp table environments */
    if (bdb_state->attr->temp_table_clean_exit) {
        if (gbl_temptable_pool_capacity > 0)
            comdb2_objpool_destroy(bdb_state->temp_table_pool);
        else {
            last = 0;
            while (!rc && last == 0) {
                rc =
                    bdb_temp_table_destroy_lru(NULL, bdb_state, &last, &bdberr);
            }
        }
    }

    free(bdb_state->origname);
    free(bdb_state->name);
    free(bdb_state->dir);
    free(bdb_state->txndir);
    free(bdb_state->tmpdir);

    free(bdb_state->seqnum_info->seqnums);
    free(bdb_state->last_downgrade_time);
    free(bdb_state->master_lease);
    free(bdb_state->coherent_state);
    free(bdb_state->seqnum_info->waitlist);
    free(bdb_state->seqnum_info->trackpool);
    free(bdb_state->seqnum_info->time_10seconds);
    free(bdb_state->seqnum_info->time_minute);
    free(bdb_state->seqnum_info->expected_udp_count);
    free(bdb_state->seqnum_info->incomming_udp_count);
    free(bdb_state->seqnum_info->udp_average_counter);
    free(bdb_state->seqnum_info->filenum);

    free(bdb_state->repinfo->appseqnum);

    /* We can not free bdb_state because other threads get READLOCK
     * and it does not work well doing so on freed memory, so don't:
    memset(bdb_state, 0xff, sizeof(bdb_state));
    free(bdb_state);
     */

    /* DO NOT RELEASE the write lock.  just let it be. */
    return 0;
}

int bdb_handle_reset_tran(bdb_state_type *bdb_state, tran_type *trans,
                          tran_type *cltrans)
{
    DB_TXN *tid = trans ? trans->tid : NULL;
    DB_TXN *cltid = cltrans ? cltrans->tid : NULL;

    /* Block others from assigning dbreg ID's, while we're reopening. */
    __dbreg_lock_lazy_id(bdb_state->dbenv);

    int rc = close_dbs_txn(bdb_state, cltid);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "upgrade: open_dbs as master failed\n");
        __dbreg_unlock_lazy_id(bdb_state->dbenv);
        return -1;
    }

    if (debug_switch_bdb_handle_reset_delay())
        sleep(5);

    int iammaster;
    if (bdb_state->read_write)
        iammaster = 1;
    else
        iammaster = 0;

    rc = open_dbs(bdb_state, iammaster, 1, 0, tid, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "upgrade: open_dbs as master failed\n");
        __dbreg_unlock_lazy_id(bdb_state->dbenv);
        return -1;
    }
    bdb_state->isopen = 1;

    __dbreg_unlock_lazy_id(bdb_state->dbenv);
    return 0;
}
int bdb_handle_reset(bdb_state_type *bdb_state)
{
    return bdb_handle_reset_tran(bdb_state, NULL, NULL);
}

int bdb_handle_dbp_add_hash(bdb_state_type *bdb_state, int szkb)
{
    int dtanum, strnum;
    DB *dbp;
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        for (strnum = bdb_get_datafile_num_files(bdb_state, dtanum) - 1;
             strnum >= 0; strnum--) {
            dbp = bdb_state->dbp_data[dtanum][strnum];
            if (dbp) {
                // set flag
                dbp->flags |= DB_AM_HASH;
                // add hash
                genid_hash_resize(bdb_state->dbenv, &(dbp->pg_hash), szkb);
            }
        }
    }
    return 0;
}

int bdb_handle_dbp_drop_hash(bdb_state_type *bdb_state)
{
    int dtanum, strnum;
    DB *dbp;
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        for (strnum = bdb_get_datafile_num_files(bdb_state, dtanum) - 1;
             strnum >= 0; strnum--) {
            dbp = bdb_state->dbp_data[dtanum][strnum];
            if (dbp) {
                // clear flag
                dbp->flags &= ~(DB_AM_HASH);
                // delete hash
                genid_hash_free(bdb_state->dbenv, dbp->pg_hash);
                dbp->pg_hash = NULL;
            }
        }
    }
    return 0;
}

int bdb_handle_dbp_hash_stat(bdb_state_type *bdb_state)
{
    DB *dbp;
    int dtanum, strnum;
    dbp_bthash_stat stat;
    int has_hash = 1;
    bzero(&stat, sizeof(dbp_bthash_stat));
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        for (strnum = bdb_get_datafile_num_files(bdb_state, dtanum) - 1;
             strnum >= 0; strnum--) {
            dbp = bdb_state->dbp_data[dtanum][strnum];
            if (dbp) {
                stat.n_bt_search += dbp->pg_hash_stat.n_bt_search;
                stat.n_bt_hash += dbp->pg_hash_stat.n_bt_hash;
                stat.n_bt_hash_hit += dbp->pg_hash_stat.n_bt_hash_hit;
                stat.n_bt_hash_miss += dbp->pg_hash_stat.n_bt_hash_miss;
                timeradd(&(stat.t_bt_search),
                            &(dbp->pg_hash_stat.t_bt_search),
                            &(stat.t_bt_search));
                if (!(dbp->flags & DB_AM_HASH) || !(dbp->pg_hash))
                    has_hash = 0;
            }
        }
    }

    logmsg(LOGMSG_INFO, "n_bt_search: %u\n", stat.n_bt_search);
    logmsg(LOGMSG_INFO, "n_bt_hash: %u\n", stat.n_bt_hash);
    logmsg(LOGMSG_INFO, "n_bt_hash_hit: %u\n", stat.n_bt_hash_hit);
    logmsg(LOGMSG_INFO, "n_bt_hash_miss: %u\n", stat.n_bt_hash_miss);
    logmsg(LOGMSG_INFO, "time_bt_search: %.3fms\n",
           (double)stat.t_bt_search.tv_sec * 1000 +
               (double)stat.t_bt_search.tv_usec / 1000);

    return has_hash;
}

int bdb_handle_dbp_hash_stat_reset(bdb_state_type *bdb_state)
{
    DB *dbp;
    int dtanum, strnum;
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
        for (strnum = bdb_get_datafile_num_files(bdb_state, dtanum) - 1;
             strnum >= 0; strnum--) {
            dbp = bdb_state->dbp_data[dtanum][strnum];
            if (dbp)
                bzero(&(dbp->pg_hash_stat), sizeof(dbp_bthash_stat));
        }
    }

    return 0;
}

void bdb_stop_recover_threads(bdb_state_type *bdb_state)
{
    if (bdb_state->dbenv->recovery_processors)
        thdpool_stop(bdb_state->dbenv->recovery_processors);
    if (bdb_state->dbenv->recovery_workers)
        thdpool_stop(bdb_state->dbenv->recovery_workers);
}

int bdb_close_env(bdb_state_type *bdb_state)
{
    return bdb_close_int(bdb_state, 1);
}

int berkdb_send_rtn(DB_ENV *dbenv, const DBT *control, const DBT *rec,
                    const DB_LSN *lsnp, char *host, int flags, void *usr_ptr);

void berkdb_receive_rtn(void *ack_handle, void *usr_ptr, char *from_host,
                        int usertype, void *dta, int dtalen, uint8_t is_tcp);

void berkdb_receive_test(void *ack_handle, void *usr_ptr, char *from_host,
                         int usertype, void *dta, int dtalen, uint8_t is_tcp);

void berkdb_receive_msg(void *ack_handle, void *usr_ptr, char *from_host,
                        int usertype, void *dta, int dtalen, uint8_t is_tcp);

void *watcher_thread(void *arg);
void *checkpoint_thread(void *arg);
void *logdelete_thread(void *arg);
void *memp_trickle_thread(void *arg);
void *deadlockdetect_thread(void *arg);

void make_lsn(DB_LSN *logseqnum, unsigned int filenum, unsigned int offsetnum)
{
    logseqnum->file = filenum;
    logseqnum->offset = offsetnum;
}

void get_my_lsn(bdb_state_type *bdb_state, DB_LSN *lsnout)
{
    DB_LSN our_lsn;
    DB_LOG_STAT *log_stats;

    bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats, 0);
    make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
    free(log_stats);

    memcpy(lsnout, &our_lsn, sizeof(DB_LSN));
}

void get_master_lsn(bdb_state_type *bdb_state, DB_LSN *lsnout)
{
    char *master_host = bdb_state->repinfo->master_host;
    if (master_host != db_eid_invalid && master_host != bdb_master_dupe) {
        memcpy(lsnout, &(bdb_state->seqnum_info
                             ->seqnums[nodeix(bdb_state->repinfo->master_host)]
                             .lsn),
               sizeof(DB_LSN));
    }
}

char *lsn_to_str(char lsn_str[], DB_LSN *lsn)
{
    sprintf(lsn_str, "%d:%d", lsn->file, lsn->offset);
    return lsn_str;
}

/* packs an lsn compare type */
uint8_t *bdb_lsn_cmp_type_put(const lsn_cmp_type *p_lsn_cmp_type,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_LSN_CMP_TYPE_LEN > p_buf_end - p_buf)
        return NULL;

    p_buf = buf_put(&(p_lsn_cmp_type->lsn.file),
                    sizeof(p_lsn_cmp_type->lsn.file), p_buf, p_buf_end);
    p_buf = buf_put(&(p_lsn_cmp_type->lsn.offset),
                    sizeof(p_lsn_cmp_type->lsn.offset), p_buf, p_buf_end);
    p_buf = buf_put(&(p_lsn_cmp_type->delta), sizeof(p_lsn_cmp_type->delta),
                    p_buf, p_buf_end);

    return p_buf;
}

/* gets an lsn compare type */
const uint8_t *bdb_lsn_cmp_type_get(lsn_cmp_type *p_lsn_cmp_type,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_LSN_CMP_TYPE_LEN > p_buf_end - p_buf)
        return NULL;

    p_buf = buf_get(&(p_lsn_cmp_type->lsn.file),
                    sizeof(p_lsn_cmp_type->lsn.file), p_buf, p_buf_end);
    p_buf = buf_get(&(p_lsn_cmp_type->lsn.offset),
                    sizeof(p_lsn_cmp_type->lsn.offset), p_buf, p_buf_end);
    p_buf = buf_get(&(p_lsn_cmp_type->delta), sizeof(p_lsn_cmp_type->delta),
                    p_buf, p_buf_end);

    return p_buf;
}

/* but don't be verbose unless something changes. */
static int print_catchup_message(bdb_state_type *bdb_state, int phase,
                                 DB_LSN *our_lsn, DB_LSN *master_lsn,
                                 uint64_t *gap, uint64_t *prev_gap,
                                 int *prev_state, int starting_time,
                                 DB_LSN *starting_lsn)
{
    char our_lsn_str[80];
    char master_lsn_str[80];
    DB_LSN rep_verify_lsn, rep_verify_start_lsn;
    uint8_t p_lsn_cmp[BDB_LSN_CMP_TYPE_LEN], *p_buf, *p_buf_end;
    int rc;
    int doing_rep_verify = 0;

    uint64_t our =
        ((uint64_t)(our_lsn->file) << 32) + (uint64_t)our_lsn->offset;
    uint64_t master =
        ((uint64_t)(master_lsn->file) << 32) + (uint64_t)master_lsn->offset;
    *gap = (our >= master ? 0 : master - our);
    int state;
    lsn_cmp_type lsn_cmp;

    /* First time through all is well. */
    if (*prev_gap == 0) {
        *prev_gap = *gap + 1;
        *prev_state = -2;
    }

    if (*gap < *prev_gap)
        state = 1;
    else {
        if (*gap == *prev_gap)
            state = 0;
        else
            state = -1;
    }

    if (state != *prev_state) {
        logmsg(LOGMSG_WARN, "\n");
        logmsg(LOGMSG_WARN, 
               "I am catching up with updates that occured while I was down.\n");
        if (state > 0) {
            logmsg(LOGMSG_WARN, "I am making progress and should be allowed to continue.\n");
            logmsg(LOGMSG_WARN, "DO NOT MARK THIS MACHINE ONLINE UNTIL I HAVE CAUGHT UP.\n");
        } else {
            bdb_state->dbenv->get_rep_verify_lsn(
                bdb_state->dbenv, &rep_verify_lsn, &rep_verify_start_lsn);
            if (rep_verify_lsn.file)
                doing_rep_verify = 1;

            if (rep_verify_lsn.file != 0) {
                logmsg(LOGMSG_WARN, "sending COMMITDELAYMORE to %s\n",
                        bdb_state->repinfo->master_host);

                rc = net_send(bdb_state->repinfo->netinfo,
                              bdb_state->repinfo->master_host,
                              USER_TYPE_COMMITDELAYMORE, NULL, 0, 1);

                if (rc != 0) {
                    logmsg(LOGMSG_WARN, "failed to send COMMITDELAYMORE to %s rc: %d\n",
                            bdb_state->repinfo->master_host, rc);
                    return -1;
                }
            }

            if (state == 0) {
                if (rep_verify_lsn.file) {
                    logmsg(LOGMSG_WARN, "I AM ROLLING BACK MY LOGS.\n");
                    logmsg(LOGMSG_WARN, "I AM at %u:%u, started at %u:%u, %" PRIu64
                                    " rolled back.\n",
                            rep_verify_lsn.file, rep_verify_lsn.offset,
                            rep_verify_start_lsn.file,
                            rep_verify_start_lsn.offset,
                            subtract_lsn(bdb_state, &rep_verify_start_lsn,
                                         &rep_verify_lsn));
                } else
                    logmsg(LOGMSG_WARN, "I AM NOT MAKING ANY PROGRESS.\n");
            } else {
                logmsg(LOGMSG_USER,
                       "%s line %d calling rep_start as client with egen 0\n",
                       __func__, __LINE__);
                rc = bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, 0,
                                                 DB_REP_CLIENT);

                logmsg(LOGMSG_WARN, "I AM FALLING FURTHER BEHIND THE MASTER NODE.\n"); 
            }
            logmsg(LOGMSG_WARN, "IF I DO NOT START MAKING PROGRESS SOON THEN THERE "
                            "MAY BE A PROBLEM.\n");
        }

        logmsg(LOGMSG_WARN, "\n");
    }

    uint64_t behind = subtract_lsn(bdb_state, master_lsn, our_lsn);
    logmsg(LOGMSG_WARN,
           "catching up (%d):: us: %s "
           " master : %s behind %" PRIu64 "mb (%" PRIu64 " bytes)\n",
           phase, lsn_to_str(our_lsn_str, our_lsn),
           lsn_to_str(master_lsn_str, master_lsn),
           behind / (1024 * 1024), behind);

    lsn_cmp.lsn.file = our_lsn->file;
    lsn_cmp.lsn.offset = our_lsn->offset;
    lsn_cmp.delta = bdb_state->attr->logfiledelta;

    p_buf = p_lsn_cmp;
    p_buf_end = p_lsn_cmp + BDB_LSN_CMP_TYPE_LEN;
    bdb_lsn_cmp_type_put(&lsn_cmp, p_buf, p_buf_end);

    rc = net_send_message(bdb_state->repinfo->netinfo,
                          bdb_state->repinfo->master_host, USER_TYPE_LSNCMP,
                          p_lsn_cmp, sizeof(lsn_cmp_type), 1, 60 * 1000);

    *prev_gap = *gap;
    *prev_state = state;

    /* if its been 5 minutes, and we havent moved forward more than 10 megs,
       kill ourselves */
    if (!doing_rep_verify && bdb_state->attr->rep_verify_limit_enabled && (time(NULL) - starting_time) > bdb_state->attr->rep_verify_max_time) {
        if (subtract_lsn(bdb_state, our_lsn, starting_lsn) < bdb_state->attr->rep_verify_min_progress) {
            logmsg(LOGMSG_FATAL, "made less then %d byte progress in %d seconds, exiting\n",
                    bdb_state->attr->rep_verify_min_progress, bdb_state->attr->rep_verify_max_time);
            exit(1);
        }
    }

    return 0;
}

void rep_all_req(bdb_state_type *bdb_state) { return; }

/*
  returns the number of bytes between lsn2 and lsn1.
  lsn2 - lsn1
  */
uint64_t subtract_lsn(bdb_state_type *bdb_state, DB_LSN *lsn2, DB_LSN *lsn1)
{
    uint64_t num_bytes;
    num_bytes = (uint64_t)(lsn2->file - lsn1->file) *
                (uint64_t)bdb_state->attr->logfilesize;
    num_bytes += lsn2->offset;
    num_bytes -= lsn1->offset;
    return num_bytes;
}

/*
static void print_ourlsn(bdb_state_type *bdb_state)
{
   DB_LSN our_lsn;
   DB_LOG_STAT *log_stats;
   char our_lsn_str[80];

   bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats, 0);
   make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
   free(log_stats);

   fprintf(stderr, "our LSN: %s\n",
      lsn_to_str(our_lsn_str, &our_lsn));
}
*/

static void bdb_admin_appsock(netinfo_type *netinfo, SBUF2 *sb)
{
    bdb_state_type *bdb_state;
    bdb_state = net_get_usrptr(netinfo);

    if (bdb_state->callback->admin_appsock_rtn)
        (bdb_state->callback->admin_appsock_rtn)(bdb_state, sb);
}

static void bdb_appsock(netinfo_type *netinfo, SBUF2 *sb)
{
    bdb_state_type *bdb_state;
    bdb_state = net_get_usrptr(netinfo);

    if (bdb_state->callback->appsock_rtn)
        (bdb_state->callback->appsock_rtn)(bdb_state, sb);
}

extern int gbl_pstack_self;
void pstack_self(void)
{
    if (!gbl_pstack_self)
        return;

    char cmd[256];
    char output[20] = "/tmp/pstack.XXXXXX";
    int fd = mkstemp(output);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "%s: open(%s) err:%s\n", __func__, output, strerror(errno));
        return;
    }
    pid_t pid = getpid();
#   if defined(COMDB2_BBCMAKE) && defined(_LINUX_SOURCE)
    snprintf(cmd, sizeof(cmd), "/opt/bbinfra/bin/pstack %d > %s", pid, output);
#   else
    snprintf(cmd, sizeof(cmd), "pstack %d > %s", pid, output);
#   endif
    errno = 0;
    int rc = system(cmd);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d system(\"%s\") failed (rc = %d)\n", __func__, __LINE__, cmd, rc);
        close(fd);
        unlink(output);
        return;
    }

    FILE *out = fdopen(fd, "r");
    if (!out) {
        logmsg(LOGMSG_ERROR, "%s: open(%s) err:%s\n", __func__, cmd, strerror(errno));
        close(fd);
        unlink(output);
        return;
    }
    int old = gbl_logmsg_ctrace;
    gbl_logmsg_ctrace = 1;
    char line[LINE_MAX];
    while (fgets(line, sizeof(line), out) == line) {
        logmsg(LOGMSG_USER, "%s", line);
    }
    gbl_logmsg_ctrace = old;
    fclose(out);
    unlink(output);
}

static void panic_func(DB_ENV *dbenv, int errval)
{
    bdb_state_type *bdb_state = dbenv->app_private;
    if (bdb_state->parent) bdb_state = bdb_state->parent;

    Pthread_mutex_lock(&(bdb_state->exit_lock));

    pstack_self();
    logmsg(LOGMSG_FATAL, "PANIC: comdb2 shutting down error:%d\n", errval);

    /* this code sometimes deadlocks.  install a timer - if it fires, we
       abort.  We don't lose much since we are about to exit anyway. */
    signal(SIGALRM, SIG_DFL);
    alarm(15);

    /* Take a full diagnostic snapshot.  Disable the panic logic for this
     * to work. */
    if (bdb_state->attr->panic_fulldiag) {
        char buf[100];
        int len = snprintf(buf, sizeof(buf), "f %s/panic_full_diag fulldiag", bdb_state->dir);
        logmsg(LOGMSG_FATAL, "PANIC: running bdb '%s' command to grab diagnostics\n", buf);
        dbenv->set_flags(dbenv, DB_NOPANIC, 1);
        bdb_process_user_command(bdb_state, buf, len, 0);
        dbenv->set_flags(dbenv, DB_NOPANIC, 0);
    }
    alarm(0);
    abort();
}

static void net_hello_rtn(struct netinfo_struct *netinfo, char name[])
{
    bdb_state_type *bdb_state;

    bdb_state = net_get_usrptr(netinfo);

    logmsg(LOGMSG_DEBUG, "net_hello_rtn got hello from <%s>\n", name);

    if (strcmp(bdb_state->name, name) != 0) {
        logmsg(LOGMSG_FATAL, "crossed clusters!  hello from <%s>\n", name);
        exit(1);
    }
}

static void set_dbenv_stuff(DB_ENV *dbenv, bdb_state_type *bdb_state)
{
    int rc;

    rc = dbenv->set_lk_max_objects(dbenv, bdb_state->attr->maxlockobjects);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set_lk_max_objects failed\n");
        exit(1);
    }

    rc = dbenv->set_lk_max_locks(dbenv, bdb_state->attr->maxlocks);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set_lk_max_locks failed\n");
        exit(1);
    }

    rc = dbenv->set_lk_max_lockers(dbenv, bdb_state->attr->maxlockers);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set_lk_max_lockers failed\n");
        exit(1);
    }

    rc = dbenv->set_tx_max(dbenv, bdb_state->attr->maxtxn);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set_txn_max\n");
        exit(1);
    }

    rc = dbenv->set_app_dispatch(dbenv, bdb_apprec);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set_app_dispatch\n");
        exit(1);
    }

    rc = dbenv->set_lsn_chaining(dbenv, bdb_state->attr->rep_lsn_chaining);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set_lsn_chaining\n");
        exit(1);
    }
}

/* spawn off thread that does updbackup and autoanalyze */
void create_udpbackup_analyze_thread(bdb_state_type *bdb_state)
{
    if (gbl_exit) return;
#   if 0
    if (gbl_libevent) {
        add_timer_event(udp_backup, bdb_state, 500);
        add_timer_event(auto_analyze, bdb_state, bdb_state->attr->chk_aa_time * 1000);
        return;
    }
#   endif
    pthread_t thread_id;
    pthread_attr_t thd_attr;
    logmsg(LOGMSG_INFO, "starting udpbackup_and_autoanalyze_thd thread\n");
    Pthread_attr_init(&thd_attr);
    Pthread_attr_setstacksize(&thd_attr, 128 * 1024); /* 4K */
    Pthread_attr_setdetachstate(&thd_attr, PTHREAD_CREATE_DETACHED);
    Pthread_create(&thread_id, &thd_attr, udpbackup_and_autoanalyze_thd, bdb_state);
}

int gbl_passed_repverify = 0;

static pthread_mutexattr_t bdb_recursive_mutex;

/* unsigned bytes -> pretty printed string */
static char *prettysz(uint64_t s, char *b)
{
    double sz = s;
    if (sz >= 1024 * 1024 * 1024) {
        sprintf(b, "%gGB", sz / (1024 * 1024 * 1024));
    } else if (sz >= 1024 * 1024) {
        sprintf(b, "%gMB", sz / (1024 * 1024));
    } else if (sz >= 1024) {
        sprintf(b, "%gKB", sz / 1024);
    } else {
        sprintf(b, "%gBytes", sz / 1024);
    }
    return b;
}

extern int gbl_rowlocks;

extern int comdb2_is_standalone(DB_ENV *dbenv);
extern int comdb2_reload_schemas(DB_ENV *dbenv, DB_LSN *lsn);
extern int comdb2_replicated_truncate(DB_ENV *dbenv, DB_LSN *lsn,
                                      uint32_t flags);
extern int comdb2_recovery_cleanup(DB_ENV *dbenv, DB_LSN *lsn, int is_master);

int bdb_is_standalone(void *dbenv, void *in_bdb_state)
{
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    if (!bdb_state || !bdb_state->repinfo)
        return 1;
    return net_is_single_sanctioned_node(bdb_state->repinfo->netinfo);
}

extern int gbl_commit_delay_trace;
int gbl_skip_catchup_logic = 0;

static DB_ENV *dbenv_open(bdb_state_type *bdb_state)
{
    DB_ENV *dbenv;
    int rc;
    int startasmaster;
    int flags;
    char *master_host;
    char txndir[100];
    int count;
    DB_LSN master_lsn;
    DB_LSN our_lsn;
    DB_LOG_STAT *log_stats;
    char our_lsn_str[80];
    seqnum_type master_seqnum;
    uint64_t gap = 0, prev_gap = 0;
    int catchup_state = 0;
    DB_LSN starting_lsn;
    int starting_time;
    char *myhost;
    int is_early;

    count = 0;

    bb_berkdb_thread_stats_init();
    myhost = net_get_mynode(bdb_state->repinfo->netinfo);

    if (!is_real_netinfo(bdb_state->repinfo->netinfo) ||
        bdb_state->attr->i_am_master) {
        /*fprintf(stderr, "we will start as master of fake replication
         * group\n");*/
        startasmaster = 1;
        set_repinfo_master_host(bdb_state, myhost, __func__, __LINE__);
    } else {
        startasmaster = 0;
        set_repinfo_master_host(bdb_state, db_eid_invalid, __func__, __LINE__);
    }

    master_host = bdb_state->repinfo->master_host;

    net_set_heartbeat_check_time(bdb_state->repinfo->netinfo, 60);

    /* Create the environment handle. */
    rc = db_env_create(&dbenv, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "db_env_create: %s\n", db_strerror(rc));
        return NULL;
    }

    rc = dbenv->set_paniccall(dbenv, panic_func);

    extern int gbl_is_physical_replicant;
    if (gbl_is_physical_replicant && physrep_ignore_table_count() > 0) {
        rc = dbenv->set_rep_ignore(dbenv, physrep_ignore_btree);
    }

#ifdef BDB_VERB_REPLICATION_DEFAULT
    /* turn on verbose replication by default, so I can see what's happening
     * before the mtrap is enabled. */
    rc = dbenv->set_verbose(dbenv, DB_VERB_REPLICATION, 1);
#else
    rc = dbenv->set_verbose(dbenv, DB_VERB_REPLICATION, 0);
#endif

    /* errors to stderr */
    /*fprintf(stderr, "setting error file to stderr\n");*/
    dbenv->set_errfile(dbenv, stderr);

    /* automatically run the deadlock detector whenever there is a
       conflict over a lock */
    if (bdb_state->attr->autodeadlockdetect) {
        int policy;

        policy = DB_LOCK_MINWRITE;

        if (bdb_state->attr->deadlock_most_writes)
            policy = DB_LOCK_MAXWRITE;
        if (bdb_state->attr->deadlock_youngest_ever)
            policy = DB_LOCK_YOUNGEST_EVER;

        rc = dbenv->set_lk_detect(dbenv, policy);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "set_lk_detect failed\n");
            return NULL;
        }

        if (bdb_state->attr->deadlock_least_writes) {
            rc = dbenv->set_deadlock_override(dbenv, DB_LOCK_MINWRITE_NOREAD);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "set_deadlock_override failed\n");
                return NULL;
            }
        }
        if (bdb_state->attr->deadlock_least_writes_ever) {
            rc = dbenv->set_deadlock_override(dbenv, DB_LOCK_MINWRITE_EVER);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "set_deadlock_override failed\n");
                return NULL;
            }
        }
    }

    if (!bdb_state->attr->synctransactions) {
        rc = dbenv->set_flags(dbenv, DB_TXN_NOSYNC, 1);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "set_flags failed\n");
            exit(1);
        }
    } else {
        rc = dbenv->set_flags(dbenv, DB_TXN_NOSYNC, 0);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "set_flags failed\n");
            exit(1);
        }
    }

    if (bdb_state->attr->directio) {
        /* check if we can read write page in direct io mode */
        rc = bdb_watchdog_test_io_dir(bdb_state, bdb_state->dir);
        if(rc && EINVAL == errno) {
            logmsg(LOGMSG_FATAL, "Direct IO is not supported for dir %s\n"
                    "Please set 'setattr directio 0' in lrl\n", bdb_state->dir);
            exit(1);
        }

        rc = dbenv->set_flags(dbenv, DB_DIRECT_DB, 1);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "set flags DB_DIRECT_DB failed\n");
            exit(1);
        }
        rc = dbenv->set_flags(dbenv, DB_DIRECT_LOG, 1);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "set flags DB_DIRECT_DB_LOG failed\n");
            exit(1);
        }

        /*
           rc = dbenv->set_flags(dbenv, DB_DIRECT_LOG, 1);
           if (rc != 0)
           {
           fprintf(stderr, "set flags DB_DIRECT_DB failed\n");
           exit(1);
           }
         */
    }

    if (bdb_state->attr->osync) {
        rc = dbenv->set_flags(dbenv, DB_OSYNC, 1);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "set flags DB_DIRECT_DB failed\n");
            exit(1);
        }
    }

    if (bdb_state->attr->rep_db_pagesize > 0) {
        rc =
            dbenv->set_rep_db_pagesize(dbenv, bdb_state->attr->rep_db_pagesize);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "set_rep_db_pagesize to %d failed\n",
                    bdb_state->attr->rep_db_pagesize);
            exit(1);
        }
    }

    if (bdb_state->attr->recovery_pages > 0) {
        rc = dbenv->set_mp_recovery_pages(dbenv,
                                          bdb_state->attr->recovery_pages);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "mp_set_recovery_pages failed\n");
            exit(1);
        }
    }

    rc = dbenv->set_lg_max(dbenv, bdb_state->attr->logfilesize);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set logfilesize failed\n");
        exit(1);
    }

    rc = dbenv->set_lg_bsize(dbenv, bdb_state->attr->logmemsize);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set logmemsize failed\n");
        exit(1);
    }

    rc = dbenv->set_lg_regionmax(dbenv, 0);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "set regionmax failed\n");
        exit(1);
    }

    /* Set the number of segments. */
    rc = dbenv->set_lg_nsegs(dbenv, bdb_state->attr->logsegments);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "set_lg_nsegs failed\n");
        return NULL;
    }

    /* cache size and number of cache segments */
    int ncache;
    if (bdb_state->attr->num_berkdb_caches) {
        /* we have an override - use it as the number of cache segments */
        ncache = bdb_state->attr->num_berkdb_caches;
    } else {
        /* compute the number of cache segments */
        if (bdb_state->attr->cachesize /* kb */ >
            bdb_state->attr->cache_seg_size /* mb */ * 1024)
            ncache = bdb_state->attr->cachesize /
                     (bdb_state->attr->cache_seg_size * 1024);
        else
            ncache = 1;
    }

    char b1[64], b2[64];
    logmsg(LOGMSG_INFO, "Cache:%s  Segments:%d  Segment-size:%s\n",
           prettysz(bdb_state->attr->cachesize * 1024ULL, b1), ncache,
           prettysz(bdb_state->attr->cachesize * 1024ULL / ncache, b2));

    uint32_t gbytes = bdb_state->attr->cachesize / (1024ULL * 1024);
    uint32_t bytes = 1024 * (bdb_state->attr->cachesize % (1024ULL * 1024));
    if ((rc = dbenv->set_cachesize(dbenv, gbytes, bytes, ncache)) != 0) {
        logmsg(LOGMSG_ERROR, "set_cachesize failed rc:%d\n", rc);
        return NULL;
    }
    print(bdb_state, "cache set to %d bytes\n", bdb_state->attr->cachesize);

    /* chain a pointer to bdb_state in the dbenv */
    dbenv->app_private = bdb_state;

    /* chain a pointer back to dbenv in the bdb_state */
    bdb_state->dbenv = dbenv;

    /*
      open our environment with the following flags:
         - create all necessary files
         - run recovery before we proceed
         - disable multiprocess (shared memory) ability
         - allow use of multiple threads in this process
         - init all required berkeley db subsystems to give us
           transactions and replication
    */

    flags = DB_CREATE | DB_PRIVATE | DB_THREAD | DB_INIT_LOCK | DB_INIT_LOG |
            DB_INIT_MPOOL | DB_INIT_TXN | DB_INIT_REP;

    /* register the routine that berkeley db will use to send data
       over the network */
    dbenv->set_rep_transport(dbenv, bdb_state->repinfo->myhost,
                             berkdb_send_rtn);

    dbenv->set_check_standalone(dbenv, comdb2_is_standalone);
    dbenv->set_truncate_sc_callback(dbenv, comdb2_reload_schemas);
    dbenv->set_rep_truncate_callback(dbenv, comdb2_replicated_truncate);
    dbenv->set_rep_recovery_cleanup(dbenv, comdb2_recovery_cleanup);

    /* Register logical start and commit functions */
    dbenv->set_logical_start(dbenv, berkdb_start_logical);
    dbenv->set_logical_commit(dbenv, berkdb_commit_logical);

    /* register our environments name for sanity checking purposes. */
    logmsg(LOGMSG_INFO, "registering <%s> with net code\n", bdb_state->name);
    net_register_name(bdb_state->repinfo->netinfo, bdb_state->name);

    /* register our routine that net will call when it gets a hello
       msg with the name that was registered by the node that generated
       the hello msg.  if its a different name, we have crossed clusters.
       panic and exit */
    net_register_hello(bdb_state->repinfo->netinfo, net_hello_rtn);

    /* register the routine that will delivered data from the
       network to berkeley db */
    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_BERKDB_REP,
                         "berkdb_replication", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_BERKDB_NEWSEQ,
                         "berkdb_newseq", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_COMMITDELAYMORE,
                         "commitdelaymore", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_COMMITDELAYNONE,
                         "commitdelaynone", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_COMMITDELAYTIMED, "commitdelaytimed",
                         berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo,
                         USER_TYPE_MASTERCMPCONTEXTLIST, "mastercmpcontextlist",
                         berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_GBLCONTEXT,
                         "globalcontext", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_BERKDB_FILENUM,
                         "berkdbfilenum", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_TEST, "test",
                         berkdb_receive_test);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_ADD, "add",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_ADD_NAME,
                         "add_name", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_DEL, "del",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_DEL_NAME,
                         "del_name", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_DECOM_DEPRECATED, "decom",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_DECOM_NAME_DEPRECATED,
                         "decom_name", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_ADD_DUMMY,
                         "add_dummy", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_LSNCMP,
                         "lsncmp", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_TRANSFERMASTER,
                         "transfermaster", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo,
                         USER_TYPE_TRANSFERMASTER_NAME, "tranfermaster_name",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_REPTRC,
                         "reptrc", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo,
                         USER_TYPE_DOWNGRADEANDLOSE, "downgradeandlose",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_INPROCMSG,
                         "inprocmsg", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_GETCONTEXT,
                         "getcontext", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_HEREISCONTEXT,
                         "hereiscontext", berkdb_receive_rtn);

    net_register_handler(bdb_state->repinfo->netinfo,
                         USER_TYPE_YOUARENOTCOHERENT, "youarenotcoherent",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_YOUARECOHERENT,
                         "youarecoherent", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_TCP_TIMESTAMP,
                         "tcp_timestamp", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo,
                         USER_TYPE_TCP_TIMESTAMP_ACK, "tcp_timestamp_ack",
                         berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_PING_TIMESTAMP,
                         "ping_timestamp", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_ANALYZED_TBL,
                         "analyzed_tbl", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_COHERENCY_LEASE,
                         "coherency_lease", receive_coherency_lease);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_REQ_START_LSN,
                         "req_start_lsn", receive_start_lsn_request);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_PAGE_COMPACT,
                         "page_compact", berkdb_receive_msg);

    net_register_handler(bdb_state->repinfo->netinfo, USER_TYPE_TRUNCATE_LOG,
                         "truncate_log", berkdb_receive_msg);
    /* register our net library appsock wedge.  this lets us return
       the usr ptr containing the bdb state to the caller instead
       of the netinfo pointer */
    net_register_appsock(bdb_state->repinfo->netinfo, bdb_appsock);
    net_register_admin_appsock(bdb_state->repinfo->netinfo, bdb_admin_appsock);

    /* register the routine that will be called when a sock closes*/
    net_register_hostdown(bdb_state->repinfo->netinfo, net_hostdown_rtn);

    /* register the routine that will be called when a new node is
       added dynamically */
    net_register_newnode(bdb_state->repinfo->netinfo, net_newnode_rtn);

    /* register the routine that will be called when a new thread
       starts that might call into bdb lib */
    net_register_start_thread_callback(bdb_state->repinfo->netinfo,
                                       net_startthread_rtn);

    /* register the routine that will be called when a new thread
       starts that might call into bdb lib */
    net_register_stop_thread_callback(bdb_state->repinfo->netinfo,
                                      net_stopthread_rtn);

    /* register a routine which will re-order the out-queue to
       be in lsn order */
    net_register_netcmp(bdb_state->repinfo->netinfo, net_cmplsn_rtn);

    net_register_getlsn(bdb_state->repinfo->netinfo, net_getlsn_rtn);

    /* Register logput throttle function */
    net_rep_throttle_init(bdb_state->repinfo->netinfo);

    /* Register qstat if its enabled */
    net_rep_qstat_init(bdb_state->repinfo->netinfo);

    /* set the callback data so we get our bdb_state pointer from these
     * calls. */
    net_set_callback_data(bdb_state->repinfo->netinfo, bdb_state);

    /* tell berkdb to start its replication subsystem */
    flags |= DB_INIT_REP;

    if (gbl_rowlocks)
        flags |= DB_ROWLOCKS;

    if (bdb_state->attr->fullrecovery) {
        logmsg(LOGMSG_INFO, "running full recovery\n");
        flags |= DB_RECOVER_FATAL;
    } else
        flags |= DB_RECOVER;

    set_dbenv_stuff(dbenv, bdb_state);

    /* now open the environment */

    if (bdb_state->attr->nonames)
        sprintf(txndir, "XXX.logs");
    else
        sprintf(txndir, "XXX.%s.txn", bdb_state->name);

    /* these things need to be set up for logical recovery which will
       happen as soon as we call dbenv->open */
    if (gbl_temptable_pool_capacity > 0) {
        rc = comdb2_objpool_create_lifo(
            &bdb_state->temp_table_pool, "temp table",
            gbl_temptable_pool_capacity, bdb_temp_table_create_pool_wrapper,
            bdb_state, bdb_temp_table_destroy_pool_wrapper, bdb_state,
            bdb_temp_table_notify_pool_wrapper, bdb_state);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "failed to create temp table pool\n");
            exit(1);
        }
        logmsg(LOGMSG_INFO, "Temptable pool enabled.\n");
    }

    Pthread_mutex_init(&(bdb_state->temp_list_lock), NULL);
    bdb_state->logical_transactions_hash = hash_init_o(
        offsetof(tran_type, logical_tranid), sizeof(unsigned long long));
    Pthread_cond_init(&(bdb_state->temptable_wait), NULL);
    bdb_state->temp_stats = calloc(1, sizeof(*(bdb_state->temp_stats)));
    pthread_mutexattr_init(&bdb_recursive_mutex);
    pthread_mutexattr_settype(&bdb_recursive_mutex, PTHREAD_MUTEX_RECURSIVE);
    Pthread_mutex_init(&bdb_state->translist_lk, &bdb_recursive_mutex);
    listc_init(&bdb_state->logical_transactions_list,
               offsetof(struct tran_tag, tranlist_lnk));

    /*init this to the first possible log record in the db, we reset it later*/
    bdb_state->lwm.file = 1;
    bdb_state->lwm.offset = 0;
    bdb_state->after_llmeta_init_done = 0;
    dbenv->set_num_recovery_processor_threads(dbenv,
                                              bdb_state->attr->rep_processors);
    dbenv->set_num_recovery_worker_threads(dbenv, bdb_state->attr->rep_workers);
    dbenv->set_recovery_memsize(dbenv, bdb_state->attr->rep_memsize);
    dbenv->set_page_extent_size(dbenv, bdb_state->attr->page_extent_size);
    dbenv->set_comdb2_dirs(dbenv, bdb_state->dir, bdb_state->txndir,
                           bdb_state->tmpdir);

    extern char *gbl_crypto;
    if (gbl_crypto) {
        FILE *crypto = fopen(gbl_crypto, "r");
        if (crypto == NULL) {
            logmsg(LOGMSG_ERROR, "%s fopen(%s) errno:%d (%s)\n", __func__,
                    gbl_crypto, errno, strerror(errno));
            exit(1);
        }
        char passwd[1024];
        if ((fgets(passwd, sizeof(passwd), crypto)) == NULL) {
            logmsg(LOGMSG_ERROR, 
                   "%s fgets returned NULL -- ferror:%d feof:%d errno:%d (%s)\n",
                   __func__, ferror(crypto), feof(crypto), errno, strerror(errno));
            exit(1);
        }
        fclose(crypto);
        if ((rc = dbenv->set_encrypt(dbenv, passwd, DB_ENCRYPT_AES)) != 0) {
            logmsg(LOGMSG_FATAL, "%s set_encrypt rc:%d\n", __func__, rc);
            exit(1);
        }
        memset(passwd, 0xff, sizeof(passwd));
        // unlink(gbl_crypto);
        memset(gbl_crypto, 0xff, strlen(gbl_crypto));
        free(gbl_crypto);
        // gbl_crypto = NULL; /* don't set to null (used as bool flag) */
        logmsg(LOGMSG_INFO, "DB FILES AND LOGS WILL BE ENCRYPTED\n");
    }

    {
        char *fname;
        fname = comdb2_location("marker", "%s.iomap", bdb_state->name);
        dbenv->setattr(dbenv, "iomapfilename", fname, 0);
        free(fname);
    }

    if (bdb_state->recoverylsn) {
        DB_LSN lsn;
        int rc;
        rc = sscanf(bdb_state->recoverylsn, "%u:%u", &lsn.file, &lsn.offset);
        if (rc != 2) {
            logmsg(LOGMSG_FATAL, "Invalid LSN %s in recovery options\n",
                    bdb_state->recoverylsn);
            exit(1);
        }

        rc = dbenv->set_recovery_lsn(bdb_state->dbenv, &lsn);
        if (rc) {
            logmsg(LOGMSG_FATAL, "Failed to set recovery LSN %s rc %ds\n",
                    bdb_state->recoverylsn, rc);
            exit(1);
        }
    }

    struct deferred_berkdb_option *opt;
    opt = listc_rtl(&bdb_state->attr->deferred_berkdb_options);
    while (opt) {
        bdb_berkdb_set_attr(bdb_state, opt->attr, opt->value, opt->ivalue);
        free(opt->attr);
        free(opt->value);
        free(opt);
        opt = listc_rtl(&bdb_state->attr->deferred_berkdb_options);
    }

    BDB_WRITELOCK("dbenv_open");

    print(bdb_state, "opening %s\n", txndir);
    rc = dbenv->open(dbenv, txndir, flags, S_IRUSR | S_IWUSR);
    if (rc != 0) {
        (void)dbenv->close(dbenv, 0);
        logmsg(LOGMSG_FATAL, "%d dbenv->open: %s: %s\n", rc, bdb_state->name,
                db_strerror(rc));
        exit(1);
    }

    BDB_RELLOCK();

    /* Just before we are officially "open" - we still need to add any blkseqs
     * that may precede the recovery point in the log.  Now would be a good time
     * -
     * the environment is open, but we haven't started replication yet. */
    if (bdb_state->attr->private_blkseq_enabled) {
        rc = bdb_recover_blkseq(bdb_state);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_recover_blkseq rc %d\n", rc);
            return NULL;
        }
    }

/* skip all the replication stuff if we dont have a network */
/*
if (!is_real_netinfo(bdb_state->repinfo->netinfo))
   goto end;
   */

/* limit amount we retrans in one shot */
    rc = dbenv->set_rep_limit(dbenv, 0, bdb_state->attr->replimit);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "%s: dbenv->set_rep_limit(%u) %d %s\n", __func__,
                bdb_state->attr->replimit, rc, bdb_strerror(rc));
        exit(1);
    }

#ifdef DEAD
    /* immediately ask for retrans if we see a gap in sequence */
    dbenv->set_rep_request(dbenv, 1, 1);
#endif

    /* display our starting LSN before we begin replication */
    {
        DB_LSN our_lsn;
        DB_LOG_STAT *log_stats;
        char our_lsn_str[80];

        bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats,
                                   DB_STAT_VERIFY);
        make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
        free(log_stats);

        print(bdb_state, "BEFORE REP_START our LSN: %s\n",
              lsn_to_str(our_lsn_str, &our_lsn));

        memcpy(&starting_lsn, &our_lsn, sizeof(DB_LSN));
        starting_time = time(NULL);
    }

    /* start the network up */
    print(bdb_state, "starting network\n");
    rc = net_init(bdb_state->repinfo->netinfo);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "init_network failed\n");
        exit(1);
    }

    start_udp_reader(bdb_state);

    if (startasmaster) {
        logmsg(LOGMSG_INFO,
               "%s line %d calling rep_start as master with egen 0\n", __func__,
               __LINE__);
        rc = dbenv->rep_start(dbenv, NULL, 0, DB_REP_MASTER);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "dbenv_open: rep_start as master failed %d %s\n",
                    rc, db_strerror(rc));
            return NULL;
        }
        print(bdb_state, "dbenv_open: started rep as MASTER\n");
    } else /* we start as a client */
    {
        /*fprintf(stderr, "dbenv_open: starting rep as client\n");*/
        logmsg(LOGMSG_USER,
               "%s line %d calling rep_start as client with egen 0\n", __func__,
               __LINE__);
        rc = dbenv->rep_start(dbenv, NULL, 0, DB_REP_CLIENT);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "dbenv_open: rep_start as client failed %d %s\n",
                    rc, db_strerror(rc));
            return NULL;
        }
        print(bdb_state, "dbenv_open: started rep as CLIENT\n");
    }

    if (bdb_state->rep_started) {
        logmsg(LOGMSG_ERROR, "rep_started is not 0, but i never set it!\n");
        exit(1);
    }
    bdb_state->rep_started = 1;

    /*
      fprintf(stderr, "\n\n################################ back from
      rep_start\n\n\n");
    */

    pthread_attr_t attr;
    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    Pthread_attr_setstacksize(&attr, 1024 * 1024);

    /* create the watcher thread */
    logmsg(LOGMSG_DEBUG, "creating the watcher thread\n");
    rc = pthread_create(&(bdb_state->watcher_thread), &attr, watcher_thread,
                        bdb_state);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt create watcher thread\n");
        return NULL;
    }

    Pthread_attr_destroy(&attr);

    if (0) {
        pthread_t lwm_printer_tid;
        rc = pthread_create(&lwm_printer_tid, NULL, lwm_printer_thd, bdb_state);
        if (rc) {
            logmsg(LOGMSG_DEBUG, "start lwm printer thread rc %d\n", rc);
            return NULL;
        }
    }

/*sleep(2); this is not needed anymore */

/* do not proceed untill we find a master */
waitformaster:
    while (bdb_state->repinfo->master_host == db_eid_invalid) {
        logmsg(LOGMSG_WARN, "^^^^^^^^^^^^ waiting for a master...\n");
        sleep(3);
    }

    master_host = bdb_state->repinfo->master_host;

    if ((master_host == db_eid_invalid) || (master_host == bdb_master_dupe))
        goto waitformaster;

    {
        DB_LSN our_lsn;
        DB_LOG_STAT *log_stats;
        char our_lsn_str[80];

        bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats,
                                   DB_STAT_VERIFY);

        make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);

        free(log_stats);

        print(bdb_state, "AFTER REP_START our LSN: %s\n",
              lsn_to_str(our_lsn_str, &our_lsn));
    }

    /*
       wait until _IN REALITY_ the lsn we have is pretty darn close to
       the lsn of the master
       */

again2:
    if (!gbl_skip_catchup_logic && bdb_state->repinfo->master_host != myhost) {
        /* now loop till we are close */
        master_host = bdb_state->repinfo->master_host;
        if (master_host == myhost)
            goto done2;
        if ((master_host == db_eid_invalid) || (master_host == bdb_master_dupe))
            goto waitformaster;

        memcpy(&master_seqnum,
               &(bdb_state->seqnum_info->seqnums[nodeix(master_host)]),
               sizeof(seqnum_type));
        memcpy(&master_lsn, &master_seqnum.lsn, sizeof(DB_LSN));

        rc = bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats, 0);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "err %d from log_stat\n", rc);
            exit(1);
        }

        make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
        free(log_stats);

        if (count > 10) {
            /*
               if we're in the middle of processing a message, back off
               from whining and try again the next second.  goal is not to
               commitdelaymore the master when we're actually in a long
               rep_process_message.   yes, we can race, but better than
               nothing.
            */
            if (bdb_state->repinfo->in_rep_process_message) {
                count--;
                sleep(1);
                goto again2;
            }

            rc = print_catchup_message(bdb_state, 1, &our_lsn, &master_lsn,
                                       &gap, &prev_gap, &catchup_state,
                                       starting_time, &starting_lsn);
            if (rc != 0) {
                goto again2;
            }

            count = 0;
        }
        count++;

        if (our_lsn.file != master_lsn.file) {
            sleep(1);
            goto again2;
        }

        if (our_lsn.offset > master_lsn.offset)
            goto done2;

        if ((master_lsn.offset - our_lsn.offset) >= 4096) {
            sleep(1);
            goto again2;
        }
    }
done2:
    logmsg(LOGMSG_DEBUG, "phase 2 replication catchup passed\n");

    /*
       Tell the master where we are.  he knows where he is.  when he determines
       we are "close enough" (defined by "delta") then we pass phase 3
       and go into syncronous mode.
    */

    if (!gbl_skip_catchup_logic && !bdb_state->attr->rep_skip_phase_3) {
        DB_LSN last_lsn;
        int no_change = 0;
        bzero(&last_lsn, sizeof(last_lsn));
        while (1) {
            lsn_cmp_type lsn_cmp;
            uint8_t p_lsn_cmp[BDB_LSN_CMP_TYPE_LEN], *p_buf, *p_buf_end;

            master_host = bdb_state->repinfo->master_host;
            if (master_host == myhost)
                goto done3;
            if ((master_host == db_eid_invalid) ||
                (master_host == bdb_master_dupe))
                goto waitformaster;

            rc = bdb_state->dbenv->log_stat(bdb_state->dbenv, &log_stats, 0);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "err %d from log_stat\n", rc);
                exit(1);
            }

            make_lsn(&our_lsn, log_stats->st_cur_file, log_stats->st_cur_offset);
            free(log_stats);

            if (memcmp(&last_lsn, &our_lsn, sizeof(DB_LSN)) == 0)
                no_change++;
            else
                no_change = 0;
            if (no_change > 60 / 5) {
                /* We are not making any progress.  Go back to phase 2.  This is
                 * a desparate act to try to stop the constant "stuck in phase 3"
                 * problem that we get every other day on turning the beta
                 * cluster. */

                logmsg(LOGMSG_DEBUG, "I AM STUCK IN PHASE 3!  GOING BACK TO PHASE 2\n");
                goto again2;
            }

            lsn_cmp.lsn.file = our_lsn.file;
            lsn_cmp.lsn.offset = our_lsn.offset;
            lsn_cmp.delta = 4096;

            p_buf = p_lsn_cmp;
            p_buf_end = p_lsn_cmp + BDB_LSN_CMP_TYPE_LEN;
            bdb_lsn_cmp_type_put(&lsn_cmp, p_buf, p_buf_end);

            rc = net_send_message(bdb_state->repinfo->netinfo, master_host,
                                  USER_TYPE_LSNCMP, p_lsn_cmp,
                                  sizeof(lsn_cmp_type), 1, 60 * 1000);

            if (rc == 0)
                goto done3;

            logmsg(LOGMSG_DEBUG, "catching up (3):: us LSN: %s\n",
                    lsn_to_str(our_lsn_str, &our_lsn));

            sleep(5);
        }
    done3:
        logmsg(LOGMSG_DEBUG, "phase 3 replication catchup passed\n");
    }

    /* latch state of early ack.  we need to temporarily disable it in a bit.
       thats because phase 4 relies on us sending an "ack" to a checkpoint.
       early acks wont send out a lsn messages on a checkpoint */
    is_early = gbl_early;
    gbl_early = 0;

    /* expect heartbeats from every node every 5 seconds */
    net_set_heartbeat_check_time(bdb_state->repinfo->netinfo, 10);

    /* this will make it so we start sending ACTUAL LSN values to the master
       instead of lying about our LSN (sending a MAX) which we have been doing.
       once we stop lying, we better be _damn close_ to the master, or else
       he will get slow waiting for us to catch up when he tries to commit
       a transaction. */
    bdb_state->caught_up = 1;

    /* send our real seqnum to the master now.  */

    if (bdb_state->repinfo->master_host != gbl_myhostname &&
        net_count_nodes(bdb_state->repinfo->netinfo) > 1) {
        rc = send_myseqnum_to_master(bdb_state, 1);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "error sending seqnum to master\n");
        }
    }

    int tmpnode;
    int attempts = bdb_state->attr->startup_sync_attempts;
    uint8_t *p_buf = (uint8_t *)&tmpnode;
    uint8_t *p_buf_end = ((uint8_t *)&tmpnode + sizeof(int));

    /*
      PHASE 4:
      finally now that we believe we are caught up and are no longer lying
      about our LSN to the master, lets ask the master to force the LSN
      forward and wait for us to reach the same LSN.  when we pass this
      phase, we are truly cache coherent.
      */

again:
    buf_put(&(bdb_state->repinfo->master_host), sizeof(int), p_buf, p_buf_end);

    if (!gbl_skip_catchup_logic && bdb_state->repinfo->master_host != myhost) {

        /* now we have the master checkpoint and WAIT for us to ack the seqnum,
           thus making sure we are actually LIVE */
        rc = net_send_message(
            bdb_state->repinfo->netinfo, bdb_state->repinfo->master_host,
            USER_TYPE_ADD_DUMMY, &tmpnode, sizeof(int), 1, 60 * 1000);

        /* If the master is starting, it might not have set llmeta_bdb_state
         * yet. */
        if (rc && attempts) { 
            logmsg(LOGMSG_DEBUG, "timeout on final syncup- net_send rc=%d trying again\n", rc);
            if (attempts > 0)
                attempts--;
            sleep(1);
            goto again;
        }


        if (rc != 0) {
            logmsg(LOGMSG_FATAL,
                   "net_send to %s failed rc %d- failed to sync, exiting\n",
                   bdb_state->repinfo->master_host, rc);
            exit(1);
        }
    }
    logmsg(LOGMSG_INFO, "phase 4 replication catchup passed\n");

    /* SUCCESS.  we are LIVE and CACHE COHERENT */

    /* If I'm not the master and I haven't passed rep verify, wait here. */
    while (bdb_state->repinfo->master_host != myhost && !gbl_passed_repverify) {
        sleep(1);
        logmsg(LOGMSG_DEBUG, "waiting for rep_verify to complete\n");
    }

    /* Check again if I'm still not the master. */
    if (bdb_state->repinfo->master_host != myhost) {
        rc = net_send(bdb_state->repinfo->netinfo,
                      bdb_state->repinfo->master_host,
                      USER_TYPE_COMMITDELAYNONE, NULL, 0, 1);
        if (gbl_commit_delay_trace) {
            logmsg(LOGMSG_USER, "%s line %d sending COMMITDELAYNONE\n",
                   __func__, __LINE__);
        }
    }

    /*
     * We always need to stop on a page. Otherwise we have a race condition
     * where we move off an index page and allow data to be deleted.  Then
     * we have an index entry in the bulk buffer with the wrong genid.
     */
    bdb_state->dbenv->set_bulk_stops_on_page(bdb_state->dbenv, 1);

    logmsg(LOGMSG_DEBUG, "passed_dbenv_open\n");
    bdb_state->passed_dbenv_open = 1;

    create_udpbackup_analyze_thread(bdb_state);

    /* put the early ack mode back to where it was */
    gbl_early = is_early;

    print(bdb_state, "returning from dbenv_open\n");

    return dbenv;
}

int bdb_env_init_after_llmeta(bdb_state_type *bdb_state)
{
    int rc;
    char *myhost;
    u_int32_t ltrancount;
    DB_LSN lsn;
    int bdberr;

    /* If there are outstanding logical txns run logical recovery */
    bdb_state->dbenv->ltran_count(bdb_state->dbenv, &ltrancount);

    myhost = net_get_mynode(bdb_state->repinfo->netinfo);
    if (gbl_rowlocks || ltrancount) {
        rc = bdb_get_file_lwm(bdb_state, NULL, &lsn, &bdberr);
        if (rc == 0) {
            logmsg(LOGMSG_INFO, "new lwm: %u:%u\n", lsn.file, lsn.offset);
            bdb_state->lwm = lsn;
        }

        /* Readlock here.  Logical aborts wait_for_seqnum */
        BDB_READLOCK("bdb_env_init_after_llmeta");

        if (bdb_state->repinfo->master_host == myhost) {
            logmsg(LOGMSG_INFO, "starting logical recovery as master\n");
            rc = bdb_run_logical_recovery(bdb_state, 0);
        } else {
            logmsg(LOGMSG_INFO, "starting logical recovery as replicant\n");
            rc = bdb_run_logical_recovery(bdb_state, 1);
        }

        BDB_RELLOCK();

        if (rc) {
            logmsg(LOGMSG_FATAL, "Logical recovery failed, aborting\n");
            abort();
        }
        logmsg(LOGMSG_DEBUG, "finished logical recovery rc %d\n", rc);
    }

    if (!gbl_rowlocks) {
        rc = bdb_delete_file_lwm(bdb_state, NULL, &bdberr);
    }
    bdb_state->after_llmeta_init_done = 1;
    return 0;
}


/*
 * Check if we have a low headroom in the given path 
 * if used diskspace is > than threshold returns 1
 * otherwise returns 0
 */
int has_low_headroom(const char * path, int threshold, int debug)
{
    struct statvfs stvfs;
    int rc = statvfs(path, &stvfs);
    if (rc) {
        logmsg(LOGMSG_ERROR, "statvfs on %s failed: %d %s\n", path,
                errno, strerror(errno));
        return 0;
    } 

    double pfree = ((double)stvfs.f_bavail * 100.00) / (double)stvfs.f_blocks;
    double pused = 100.0 - pfree;

    if (pused > threshold) {
        if(debug) 
           logmsg(LOGMSG_WARN, "Low headroom on %s: %f%% used > %d%% threshold\n",
                    path, pused, threshold);
        return 1;
    }

    return 0;
}


static int get_lowfilenum_sanclist(bdb_state_type *bdb_state)
{
    const char *nodes[REPMAX];
    int numnodes;
    int i;
    int lowfilenum;

    numnodes = net_get_sanctioned_node_list(bdb_state->repinfo->netinfo, REPMAX,
                                            nodes);

    lowfilenum = INT_MAX;

    for (i = 0; i < numnodes; i++) {
        if (bdb_state->seqnum_info->filenum[nodeix(nodes[i])] < lowfilenum) {
            lowfilenum = bdb_state->seqnum_info->filenum[nodeix(nodes[i])];
            if (bdb_state->attr->debug_log_deletion) {
                logmsg(LOGMSG_USER,
                       "%s set lowfilenum to %d for machine "
                       "%s\n",
                       __func__, lowfilenum, nodes[i]);
            }
        }
    }

    if (lowfilenum == INT_MAX) {
        lowfilenum = 0;
        if (bdb_state->attr->debug_log_deletion) {
            logmsg(LOGMSG_USER, "%s defaulting lowfilenum to 0\n", __func__);
        }
    }

    return lowfilenum;
}

static int get_filenum_from_logfile(char *str_in)
{
    char str_buf[80];
    char *ptr;
    int filenum;

    /* position ptr to the last '.' character in the string */
    for (ptr = str_in + strlen(str_in); *ptr != '.' && ptr >= str_in; ptr--)
        ;

    ptr++;
    strcpy(str_buf, ptr);

    filenum = atoi(str_buf);

    return filenum;
}

extern int gbl_new_snapisol_asof;

static int32_t gbl_min_truncate_file;
static int32_t gbl_min_truncate_offset;
static int32_t gbl_min_truncate_timestamp;

static pthread_rwlock_t min_trunc_lk = PTHREAD_RWLOCK_INITIALIZER;

static int bdb_calc_min_truncate(bdb_state_type *bdb_state)
{
    DB_LSN lsn;
    int rc;
    int lowfilenum;
    int32_t timestamp;
    Pthread_rwlock_wrlock(&min_trunc_lk);
    lowfilenum = get_lowfilenum_sanclist(bdb_state);
    rc = bdb_state->dbenv->mintruncate_lsn_timestamp(
        bdb_state->dbenv, lowfilenum, &lsn, &timestamp);
    if (rc == 0) {
        gbl_min_truncate_file = lsn.file;
        gbl_min_truncate_offset = lsn.offset;
        gbl_min_truncate_timestamp = timestamp;
    } else {
        gbl_min_truncate_file = 0;
        gbl_min_truncate_offset = 0;
        gbl_min_truncate_timestamp = 0;
    }
    Pthread_rwlock_unlock(&min_trunc_lk);
    return rc;
}

int bdb_dump_mintruncate_list(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->dump_mintruncate_list(bdb_state->dbenv);
}

int bdb_clear_mintruncate_list(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->clear_mintruncate_list(bdb_state->dbenv);
}

int bdb_build_mintruncate_list(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->build_mintruncate_list(bdb_state->dbenv);
}

int bdb_print_mintruncate_min(bdb_state_type *bdb_state)
{
    int32_t timestamp;
    int rc;
    DB_LSN lsn;
    rc = bdb_state->dbenv->mintruncate_lsn_timestamp(bdb_state->dbenv, 0, &lsn,
                                                     &timestamp);
    if (rc == 0) {
        logmsg(LOGMSG_USER, "[%d:%d] %u\n", lsn.file, lsn.offset, timestamp);
    }
    return rc;
}

int bdb_min_truncate(bdb_state_type *bdb_state, int *file, int *offset,
                     int32_t *timestamp)
{
    if (gbl_min_truncate_file < 1)
        bdb_calc_min_truncate(bdb_state);
    Pthread_rwlock_rdlock(&min_trunc_lk);
    if (file)
        *file = gbl_min_truncate_file;
    if (offset)
        *offset = gbl_min_truncate_offset;
    if (timestamp)
        *timestamp = gbl_min_truncate_timestamp;
    Pthread_rwlock_unlock(&min_trunc_lk);
    return 0;
}

/*
  get a list of log files we can delete
  (call DB_ENV->log_archive with no flags)
  delete the ones that are older than
  bdb_state->attr->logdeleteage
*/
static void delete_log_files_int(bdb_state_type *bdb_state)
{
    int rc;
    char **file;
    struct stat sb;
    char logname[1024];
    int low_headroom_count = 0;
    int lowfilenum;                 /* the lowest log file across the cluster */
    int local_lowfilenum = INT_MAX; /* the lowest log file of this node */
    int lwm_lowfilenum = -1;
    char **list = NULL;
    int attrlowfilenum;
    int bdberr;
    char filenums_str[512];
    DB_LSN lwmlsn;
    int numlogs;
    int lognum;
    DB_LSN snapylsn = {0};
    DB_LSN recovery_lsn;
    int is_low_headroom = 0;
    int send_filenum = 0;
    int filenum;
    int delete_adjacent;
    int ctrace_info = 0;
    int commit_lsn_map = gbl_commit_lsn_map;

    filenums_str[0] = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* dont delete any log files during a run of initcomdb2, or if the feature
     * is turned off. */
    if (bdb_state->attr->createdbs)
        return;

    /* dont delete log files during backups or hot copies */
    time_t now = time(NULL);
    if (((bdb_state->attr->logdeleteage == LOGDELETEAGE_NEVER) ||
         (bdb_state->attr->logdeleteage > now)) &&
        !has_low_headroom(bdb_state->txndir,bdb_state->attr->lowdiskthreshold, 0))
        return;

    /* get the lowest filenum of anyone in our sanc list.  we cant delete
       log files <= to that filenum */
    lowfilenum = get_lowfilenum_sanclist(bdb_state);
    if (bdb_state->attr->debug_log_deletion)
        logmsg(LOGMSG_USER, "lowfilenum %d\n", lowfilenum);

    {
        const char *hosts[REPMAX];
        int nhosts, i;
        char nodestr[128];

        nhosts = net_get_sanctioned_node_list(bdb_state->repinfo->netinfo,
                                              REPMAX, hosts);
        for (i = 0; i < nhosts; i++) {
            int filenum;
            filenum = bdb_state->seqnum_info->filenum[nodeix(hosts[i])];
            snprintf(nodestr, sizeof(nodestr), "%s:%d ", hosts[i], filenum);
            strcat(filenums_str, nodestr);
        }
    }

    extern int gbl_logical_live_sc;
    if (gbl_logical_live_sc) {
        unsigned int sc_logical_lwm = sc_get_logical_redo_lwm();
        if (sc_logical_lwm) {
            if (sc_logical_lwm < local_lowfilenum)
                local_lowfilenum = sc_logical_lwm;
            if (sc_logical_lwm < lowfilenum) {
                lowfilenum = sc_logical_lwm;
                if (bdb_state->attr->debug_log_deletion) {
                    logmsg(LOGMSG_USER, "Setting lowfilenum to %d for schema change logical redo\n", lowfilenum);
                }
            }
        }
    }

    physrep_update_low_file_num(&lowfilenum, &local_lowfilenum);

    /* debug: print filenums from other nodes */

    /* if we have a maximum filenum defined in bdb attributes which is lower,
     * use that instead. */
    attrlowfilenum = bdb_state->attr->logdeletelowfilenum;
    if (attrlowfilenum >= 0) {
        if (attrlowfilenum < lowfilenum)
            lowfilenum = attrlowfilenum;
        if (attrlowfilenum < local_lowfilenum)
            local_lowfilenum = attrlowfilenum;
    }

    /* get the filenum of our logical LWM. We can delete any log files
       lower than that */
    if (gbl_rowlocks) {
        rc = bdb_get_file_lwm(bdb_state, NULL, &lwmlsn, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't get perm lsn lwm rc %d bdberr %d\n", rc,
                    bdberr);
            return;
        }

        /* The file in lwm is the latest log file needed to run logical
           recovery. So the file before it is the newest log file that
           can be deleted. */
        if (lwmlsn.file - 1 < lowfilenum)
            lowfilenum = lwmlsn.file - 1;
        if (lwmlsn.file - 1 < local_lowfilenum)
            local_lowfilenum = lwmlsn.file - 1;
        lwm_lowfilenum = (lwmlsn.file - 1);
    }

    if (bdb_osql_trn_get_lwm(bdb_state, &snapylsn)) {
        logmsg(LOGMSG_ERROR, 
                "%s:%d failed to get snapisol/serializable lwm lsn number!\n",
                __FILE__, __LINE__);
    } else {
        if (snapylsn.file <= local_lowfilenum)
            local_lowfilenum = snapylsn.file - 1;
        if (snapylsn.file <= lowfilenum) {
            if (bdb_state->attr->debug_log_deletion) {
                logmsg(LOGMSG_USER,
                       "Setting lowfilenum to %d from %d because snapylsn is "
                       "%d:%d\n",
                       snapylsn.file - 1, lowfilenum, snapylsn.file,
                       snapylsn.offset);
            }
            lowfilenum = snapylsn.file - 1;
        } else {
            if (bdb_state->attr->debug_log_deletion) {
                logmsg(LOGMSG_USER,
                       "Ignoring snapylsn because %d is already <= %d:%d\n",
                       lowfilenum, snapylsn.file, snapylsn.offset);
            }
        }
    }

    if (gbl_new_snapisol_asof) {
        DB_LSN asoflsn;
        extern pthread_mutex_t bdb_asof_current_lsn_mutex;
        extern DB_LSN bdb_asof_current_lsn;

        Pthread_mutex_lock(&bdb_asof_current_lsn_mutex);
        asoflsn = bdb_asof_current_lsn;
        Pthread_mutex_unlock(&bdb_asof_current_lsn_mutex);

        if (asoflsn.file <= local_lowfilenum)
            local_lowfilenum = asoflsn.file - 1;
        if (asoflsn.file <= lowfilenum) {
            if (bdb_state->attr->debug_log_deletion) {
                logmsg(LOGMSG_USER,
                       "Setting lowfilenum to %d from %d because asoflsn is "
                       "%d:%d\n",
                       asoflsn.file - 1, lowfilenum, asoflsn.file,
                       asoflsn.offset);
            }
            lowfilenum = asoflsn.file - 1;
        }
    }

low_headroom:
    if (bdb_state->attr->log_delete_low_headroom_breaktime &&
        low_headroom_count >
            bdb_state->attr->log_delete_low_headroom_breaktime) {
        logmsg(LOGMSG_WARN, "low_headroom, but tried %d times and giving up\n",
               bdb_state->attr->log_delete_low_headroom_breaktime);
        return;
    }

    delete_adjacent = 1;
    /* ask berk for a list of files that it thinks we can delete */
    rc = bdb_state->dbenv->log_archive(bdb_state->dbenv, &list, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "delete_log_files: log_archive failed\n");
        return;
    }

    /* flush the current in-memory log to disk.  this will cause berkdb
       to open a new logfile for the in-memory buffer if it hasn't yet */
    if (bdb_state->attr->print_flush_log_msg)
        print(bdb_state, "flushing log file\n");
    rc = bdb_state->dbenv->log_flush(bdb_state->dbenv, NULL);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "delete_log_files: log_flush err %d\n", rc);
        return;
    }

    if (bdb_state->attr->use_recovery_start_for_log_deletion) {

        if ((rc = __db_find_recovery_start_if_enabled(bdb_state->dbenv,
                                                      &recovery_lsn)) != 0) {
            logmsg(LOGMSG_ERROR, "__db_find_recovery_start ret %d\n", rc);
            return;
        }

        if (bdb_state->attr->debug_log_deletion) {
            logmsg(LOGMSG_USER, "recovery lsn %u:%u\n", recovery_lsn.file,
                   recovery_lsn.offset);
            logmsg(LOGMSG_USER, "lowfilenum %d\n", lowfilenum);
        }
    }

    if (list != NULL) {
        int delete_hwm_logs = 0;

        for (file = list, numlogs = 0; *file != NULL; ++file)
            numlogs++;

        numlogs -= (bdb_state->attr->min_keep_logs - 1);

        if (bdb_state->attr->min_keep_logs_age_hwm &&
            numlogs > bdb_state->attr->min_keep_logs_age_hwm)
            delete_hwm_logs =
                (numlogs - bdb_state->attr->min_keep_logs_age_hwm);

        if (bdb_state->attr->log_debug_ctrace_threshold &&
            numlogs > bdb_state->attr->log_debug_ctrace_threshold)
            ctrace_info = 1;

        if (ctrace_info) {
            ctrace("Log-delete lowfilenum is %d\n", lowfilenum);
            if (snapylsn.file == lowfilenum)
                ctrace("Snapylsn is %d:%d\n", snapylsn.file, snapylsn.offset);
        }

        is_low_headroom = 0;

        for (file = list, lognum = 0; *file != NULL && lognum < numlogs;
             ++file, ++lognum) {
            logname[0] = '\0';
            sprintf(logname, "%s/%s", bdb_state->txndir, *file);

            /* extract the file number from the filename  */
            filenum = get_filenum_from_logfile(logname);

            if (bdb_state->attr->debug_log_deletion) {
                logmsg(LOGMSG_USER, "considering %s filenum %d\n", *file, filenum);
            }

            rc = stat(logname, &sb);
            if (rc != 0)
                logmsg(LOGMSG_ERROR, "delete_log_files: stat returned %d\n", rc);

            time_t log_age = time(NULL) - sb.st_mtime;

            if (log_age < bdb_state->attr->min_keep_logs_age) {
                if (delete_hwm_logs == 0) {
                    if (bdb_state->attr->debug_log_deletion)
                        logmsg(LOGMSG_ERROR,
                               "Can't delete log, age %ld not older "
                               "than log delete age %d.\n",
                               log_age, bdb_state->attr->min_keep_logs_age);
                    if (ctrace_info)
                        ctrace("Can't delete log, age %lld not older than log "
                               "delete age %lld.\n",
                               (long long int)log_age,
                               (long long int)bdb_state->attr->min_keep_logs_age);
                    break;
                }
                /* Fall through to delete */
                else {
                    if (bdb_state->attr->debug_log_deletion)
                        logmsg(LOGMSG_USER,
                               "Log age %ld is younger than min_age "
                               "but fall-through: numlogs"
                               " is %d and high water mark is %d\n",
                               log_age, numlogs,
                               bdb_state->attr->min_keep_logs_age_hwm);
                    if (ctrace_info)
                        ctrace("Log age %ld is younger than min_age but "
                               "fall-through: numlogs"
                               " is %d and high water mark is %d\n",
                               log_age, numlogs,
                               bdb_state->attr->min_keep_logs_age_hwm);
                    delete_hwm_logs--;
                }
            }

            if (!__checkpoint_ok_to_delete_log(bdb_state->dbenv, filenum)) {
                if (bdb_state->attr->debug_log_deletion)
                    logmsg(LOGMSG_USER, "not ok to delete log, newer than checkpoint\n");
                if (ctrace_info)
                    ctrace("not ok to delete log, newer than checkpoint\n");
                break;
            }

            if (recovery_lsn.file != 0 && filenum >= recovery_lsn.file) {
                if (bdb_state->attr->debug_log_deletion)
                    logmsg(LOGMSG_DEBUG, 
                           "not ok to delete log, newer than recovery point\n");
                if (ctrace_info)
                    ctrace("not ok to delete log, newer than recovery point\n");
                break;
            }

            /* If we have private blkseqs, make sure we don't delete logs that
             * contain blkseqs newer than our threshold.  */
            if (bdb_state->attr->private_blkseq_enabled &&
                !bdb_blkseq_can_delete_log(bdb_state, filenum)) {
                if (bdb_state->attr->debug_log_deletion) {
                    logmsg(LOGMSG_USER, "skipping log %s filenm %d because it has recent blkseqs\n",
                           *file, filenum);
                    bdb_blkseq_dumplogs(bdb_state);
                }
                if (ctrace_info)
                    ctrace("skipping log %s filenm %d because it has recent blkseqs\n",
                           *file, filenum);
                break;
            }

            if (lwm_lowfilenum != -1 && filenum > lwm_lowfilenum) {
                if (bdb_state->attr->debug_log_deletion)
                    logmsg(LOGMSG_USER, "not ok to delete log %d, newer than the "
                                    "lwm_lowfilenum %d\n",
                            filenum, lwm_lowfilenum);
                if (ctrace_info)
                    ctrace("not ok to delete log %d, newer than the "
                           "lwm_lowfilenum %d\n",
                           filenum, lwm_lowfilenum);
                break;
            }

            if (gbl_new_snapisol_asof) {
                /* avoid trace between reading and writting recoverable lsn */
                Pthread_mutex_lock(&bdb_gbl_recoverable_lsn_mutex);
                /* check active begin-as-of transactions */
                if (!bdb_osql_trn_asof_ok_to_delete_log(filenum)) {
                    Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
                    if (bdb_state->attr->debug_log_deletion)
                        logmsg(LOGMSG_USER, "not ok to delete log %d, log file "
                                        "needed to maintain begin-as-of "
                                        "transactions\n",
                                filenum);
                    if (ctrace_info)
                        ctrace("not ok to delete log %d, log file needed to "
                               "maintain begin-as-of transactions\n",
                               filenum);
                    break;
                }

                /* check if we still can maintain snapshot that begin as of
                 * min_keep_logs_age seconds ago */
                if (!bdb_checkpoint_list_ok_to_delete_log(
                        bdb_state->attr->min_keep_logs_age, filenum)) {
                    Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
                    if (bdb_state->attr->debug_log_deletion)
                        logmsg(LOGMSG_USER, "not ok to delete log, log file needed "
                                        "to recover to at least %ds ago\n",
                                bdb_state->attr->min_keep_logs_age);
                    if (ctrace_info)
                        ctrace("not ok to delete log, log file needed to "
                               "recover to at least %ds ago\n",
                               bdb_state->attr->min_keep_logs_age);
                    break;
                }
            }

            /* If we made it this far, we're willing to delete this file
             * locally. */
            if (filenum > send_filenum)
                send_filenum = filenum;

            /*
             * As long as the file is below our own local low number, we can
             * get rid of the new snapshot temptables. We do not need to keep those
             * around if we're only holding log files for other replicants to recover.
             */
            if (filenum <= local_lowfilenum && gbl_new_snapisol_asof)
                bdb_snapshot_asof_delete_log(bdb_state, filenum, sb.st_mtime);

            /* Delete transactions that committed in this file from the commit LSN map. */
            if (commit_lsn_map) {
                __txn_commit_map_delete_logfile_txns(bdb_state->dbenv, filenum);
            }

            if ((filenum <= lowfilenum && delete_adjacent) || is_low_headroom) {
                /* delete this file if we got this far AND it's under the
                 * replicated low number */
                if (is_low_headroom) {
                    logmsg(LOGMSG_WARN, "LOW HEADROOM : delete_log_files: deleting "
                                    "logfile: %s\n",
                            logname);
                }

                print(bdb_state, "%sdelete_log_files: deleting logfile: %s "
                                 "filenum %d lowfilenum was %d\n",
                      (is_low_headroom) ? "LOW HEADROOM : " : "", logname,
                      filenum, lowfilenum);
                print(bdb_state, "filenums: %s\n", filenums_str);
                if (gbl_rowlocks)
                    print(bdb_state, "lwm at log delete time:  %u:%u\n",
                          lwmlsn.file, lwmlsn.offset);

                int deleted = 0;

                if (gbl_backup_logfiles && bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
                    // logname includes directory so need just the filename
                    char *base = basename(logname);
                    char *newname = comdb2_location("backup_logfiles_dir", "%s", base);

                    if (bdb_state->attr->debug_log_deletion) {
                        logmsg(LOGMSG_DEBUG, "backingup log %s to %s\n", logname, newname);
                    }

                    if (ctrace_info) {
                        ctrace("backingup log %s to %s\n", logname, newname);
                    }

                    char cmd[4048];
                    int rc = snprintf(cmd, sizeof(cmd), "mv %s %s", logname, newname);
                    if (rc < sizeof(cmd)) {
                        rc = system(cmd);
                        if (rc) 
                            logmsg(LOGMSG_ERROR, "%s: Error system(\"%s\") rc = %d\n", __func__, cmd, rc);
                        else
                            deleted = 1;
                    }
                    free(newname);
                } 
                if (!deleted) {
                    if (bdb_state->attr->debug_log_deletion) {
                        logmsg(LOGMSG_DEBUG, "deleting log %s %d\n", logname, filenum);
                    }

                    if (ctrace_info) {
                        ctrace("deleting log %s %d\n", logname, filenum);
                    }

                    rc = unlink(logname);
                    if (rc) {
                        logmsg(LOGMSG_ERROR,
                               "delete_log_files: unlink for <%s> returned %d %d\n",
                               logname, rc, errno);
                    }
                }
            } else {
                /* Not done - we want to find the highest file we can delete
                 * to broadcast that around to allow others to delete it, so
                 * keep running the loop.  However, we don't want later files
                 * to become available for deletion on a later iteration of this
                 * loop, so don't actually delete so we don't create log holes.
                 */
                if (bdb_state->attr->debug_log_deletion) {
                   logmsg(LOGMSG_DEBUG, "not deleting %d, lowfilenum %d adj %d low %d\n",
                           filenum, lowfilenum, delete_adjacent,
                           is_low_headroom);
                }
                if (ctrace_info)
                    ctrace("not deleting %d, lowfilenum %d adj %d low %d\n",
                           filenum, lowfilenum, delete_adjacent,
                           is_low_headroom);
                delete_adjacent = 0;
            }

            if (gbl_new_snapisol_asof) {
                Pthread_mutex_unlock(&bdb_gbl_recoverable_lsn_mutex);
            }

            if (is_low_headroom && 
                    !has_low_headroom(bdb_state->txndir,
                        bdb_state->attr->lowdiskthreshold, 0)) {
                is_low_headroom = 0;
            } else {
                low_headroom_count++;
            }
        }

        if (has_low_headroom(bdb_state->txndir,bdb_state->attr->lowdiskthreshold, 0)) {
            low_headroom_count++;
            is_low_headroom = 1;
            free(list);
            /* try again */
            goto low_headroom;
        }

        free(list);
    }
    if (list == NULL || send_filenum == 0) {
        DB_LOGC *logc;
        DBT logrec;
        DB_LSN first_log_lsn;

        /* If there's no log files eligible for deletion, send our first log
         * number-1.
         * We already deleted it, so it's "eligible for deletion". */

        rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: can't get log cursor rc %d\n", __func__, rc);
            return;
        }
        bzero(&logrec, sizeof(DBT));
        logrec.flags = DB_DBT_MALLOC;
        rc = logc->get(logc, &first_log_lsn, &logrec, DB_FIRST);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: can't get first log record rc %d\n", __func__,
                    rc);
            logc->close(logc, 0);
            return;
        }
        if (logrec.data)
            free(logrec.data);
        logc->close(logc, 0);
        filenum = first_log_lsn.file - 1;
        send_filenum = filenum;

        if (bdb_state->attr->debug_log_deletion)
           logmsg(LOGMSG_DEBUG, "nothing to delete, at file %d\n", first_log_lsn.file);

        if (ctrace_info)
            ctrace("nothing to delete, at file %d\n", first_log_lsn.file);
    }

    /* 0 means no-one should remove any logs */
    send_filenum_to_all(bdb_state, send_filenum, 0);
    bdb_state->seqnum_info->filenum[nodeix(bdb_state->repinfo->myhost)] =
        send_filenum;
    if (bdb_state->attr->debug_log_deletion)
        logmsg(LOGMSG_WARN, "sending filenum %d\n", send_filenum);
    if (ctrace_info)
        ctrace("sending filenum %d\n", send_filenum);
}

int bdb_get_low_headroom_count(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    return bdb_state->low_headroom_count;
}

static pthread_mutex_t logdelete_lk = PTHREAD_MUTEX_INITIALIZER;
int gbl_logdelete_lock_trace = 0;

void logdelete_lock(const char *func, int line)
{
    if (gbl_logdelete_lock_trace) {
        logmsg(LOGMSG_USER, "%s line %d lock logdelete lock\n", func, line);
    }
    Pthread_mutex_lock(&logdelete_lk);
}

void logdelete_unlock(const char *func, int line)
{
    if (gbl_logdelete_lock_trace) {
        logmsg(LOGMSG_USER, "%s line %d release logdelete lock\n", func, line);
    }
    Pthread_mutex_unlock(&logdelete_lk);
}

void delete_log_files(bdb_state_type *bdb_state)
{
    extern int gbl_truncating_log;
    BDB_READLOCK("logdelete_thread");
    logdelete_lock(__func__, __LINE__);
    if (!gbl_truncating_log) {
        delete_log_files_int(bdb_state);
        bdb_calc_min_truncate(bdb_state);
    }
    logdelete_unlock(__func__, __LINE__);
    BDB_RELLOCK();
}

void bdb_print_log_files(bdb_state_type *bdb_state)
{
    int rc;
    char **list;
    char **file;
    char logname[PATH_MAX];

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = bdb_state->dbenv->log_archive(bdb_state->dbenv, &list, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_print_log_files: log_archive failed\n");
        return;
    }

    if (list != NULL) {
        for (file = list; *file != NULL; ++file) {
            logname[0] = '\0';
            snprintf(logname, sizeof(logname), "%s/%s", bdb_state->txndir,
                     *file);
            logmsg(LOGMSG_USER, "%s\n", logname);
        }

        free(list);
    }
}

/* return true if we are all on the same log file */
int rep_caught_up(bdb_state_type *bdb_state)
{
    int count;
    const char *hostlist[REPMAX];
    int i;
    int my_filenum;

    my_filenum =
        bdb_state->seqnum_info
            ->filenum[nodeix(net_get_mynode(bdb_state->repinfo->netinfo))];

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);
    for (i = 0; i < count; i++) {
        if (bdb_state->seqnum_info->filenum[nodeix(hostlist[i])] != my_filenum)
            return 0;
    }

    return 1;
}

// calculate reasonable pagesize depending on initial value in initsize,
// and record size
int calc_pagesize(int initsize, int recsize)
{
    int pagesize = initsize;
    if (recsize > 16000)
        pagesize = 65536;

    else if (recsize > 4000)
        pagesize = 32768;

    else if (recsize > 2000)
        pagesize = 16384;

    else if (recsize > 994)
        pagesize = 8192;

    /*
    fprintf(stderr, "calc_pagesize: lrl %d pagesize %d\n",
       recsize, pagesize);
    */

    return (initsize >= pagesize) ? initsize : pagesize;
}

int open_dbs(bdb_state_type *bdb_state, int iammaster, int upgrade, int create, DB_TXN *tid, uint32_t flags)
{
    int rc;
    char tmpname[PATH_MAX];
    int i;
    u_int32_t db_flags;
    int db_mode;
    int idx_flags = 0;
    unsigned int x;
    int dta_type;
    int pagesize;
    bdbtype_t bdbtype = bdb_state->bdbtype;
    int tmp_tid;
    tran_type tran = {0};

    if ((flags & BDB_OPEN_SKIP_SCHEMA_LK) == 0) {
        assert_wrlock_schema_lk();
    }

deadlock_again:
    tmp_tid = 0;

    db_flags = DB_THREAD;
    db_mode = 0666;

    if (iammaster) {
        print(bdb_state, "open_dbs: opening dbs as master\n");
        bdb_state->read_write = 1;
    } else {
        print(bdb_state, "open_dbs: opening dbs as client\n");
        bdb_state->read_write = 0;
    }

    if (tid == NULL) {
        tmp_tid = 1;
        rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "open_dbs: begin transaction failed\n");
            exit(1);
        }
    }

    tran.tid = tid;

    if ((iammaster) && (create)) {
        logmsg(LOGMSG_DEBUG, "open_dbs: running with CREATE flag\n");
        db_flags |= DB_CREATE;
        if (bdbtype == BDBTYPE_QUEUE)
            dta_type = DB_QUEUE;
        else
            dta_type = DB_BTREE;
    } else
        dta_type = DB_UNKNOWN;

    /* allow for dirty reads.  we want to allow the prefault threads to
       do dirty reads */
    /* db_flags |= DB_DIRTY_READ; */ /* This is a known enbaddener. */

    /* create/open all data files, striped or not */
    if (bdbtype == BDBTYPE_TABLE) {
        int dtanum, strnum;

        /* if we are creating a new db, we are the master, and we have a low
         * level meta table: give all the files version numbers
         * WARNING this must be done before any calls to form_file_name or any
         * other function that uses the file version db or it will result in a
         * deadlock */
        if (iammaster && create && bdb_have_llmeta()) {
            int bdberr;

            if (bdb_new_file_version_all(bdb_state, &tran, &bdberr) ||
                bdberr != BDBERR_NOERROR) {
                logmsg(LOGMSG_ERROR,
                       "bdb_open_dbs: failed to update table and its file's "
                       "version number, bdberr %d\n",
                       bdberr);
                if (tid)
                    tid->abort(tid);

                tid = NULL;
                if (tmp_tid && bdberr == BDBERR_DEADLOCK)
                    goto deadlock_again;

                return -1;
            }
        }

        for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++) {
            for (strnum = bdb_get_datafile_num_files(bdb_state, dtanum) - 1;
                 strnum >= 0; strnum--) {
                DB *dbp;

                form_datafile_name(bdb_state, tid, dtanum, strnum, tmpname,
                                   sizeof(tmpname));

                if (create) {
                    char new[PATH_MAX];
                    print(bdb_state, "deleting %s\n", bdb_trans(tmpname, new));
                    unlink(bdb_trans(tmpname, new));
                }

                rc = db_create(&dbp, bdb_state->dbenv, 0);

                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "db_create %s: %s\n", tmpname,
                            db_strerror(rc));
                    exit(1);
                }

                set_some_flags(bdb_state, dbp, tmpname);

                /*
                   new pagesize logic.  blobs always get 64k pages
                   data files get pages calculated based on lrl size.

                   all of these can be overriden with llmeta settings
                */

                if (dtanum == 0)
                    pagesize = calc_pagesize(bdb_state->attr->pagesizedta, bdb_state->lrl);
                else
                    pagesize = bdb_state->attr->pagesizeblob;

                /* get page sizes from the llmeta table if there */
                if (bdb_have_llmeta()) {
                    int rc;
                    int bdberr;
                    int llpagesize;

                    if (dtanum == 0)
                        rc = bdb_get_pagesize_alldata(&tran, &llpagesize,
                                                      &bdberr);
                    else
                        rc = bdb_get_pagesize_allblob(&tran, &llpagesize,
                                                      &bdberr);
                    if ((rc == 0) && (bdberr == 0)) {
                        if (llpagesize)
                            pagesize = llpagesize;
                    }

                    if (dtanum == 0)
                        rc = bdb_get_pagesize_data(bdb_state, &tran,
                                                   &llpagesize, &bdberr);
                    else
                        rc = bdb_get_pagesize_blob(bdb_state, &tran,
                                                   &llpagesize, &bdberr);
                    if ((rc == 0) && (bdberr == 0)) {
                        if (llpagesize)
                            pagesize = llpagesize;
                    }
                }

                if (gbl_is_physical_replicant && physrep_ignore_table(bdb_state->name)) {
                    char new[PATH_MAX];
                    print(bdb_state, "truncating ignored table %s\n", bdb_trans(tmpname, new));
                    rc = truncate(bdb_trans(tmpname, new), pagesize * 2);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR, "truncate %s error %d\n", bdb_trans(tmpname, new), errno);
                    }
                }

                rc = dbp->set_pagesize(dbp, pagesize);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "unable to set pagesize on %s to %d\n",
                            tmpname, pagesize);
                }

                print(bdb_state, "opening %s ([%d][%d])\n", tmpname,
                      dtanum, strnum);
                // dbp is datafile
                db_flags |= DB_DATAFILE;
                int iter = 0;
                do {
                    if (iter != 0)
                        poll(0, 0, 100);
                    if (dtanum == 0 /* not blob */
                        && strncasecmp(bdb_state->name, "sqlite_stat", 11) != 0)
                        /* don't compact sqlite_stat tables */
                        db_flags |= DB_OLCOMPACT;
                    rc = dbp->open(dbp, tid, tmpname, NULL, dta_type, db_flags,
                                   db_mode);
                    logmsg(
                        LOGMSG_DEBUG,
                        "dbp->open %s type=%d dbp=%p txn=%p rc %d flags=0x%X\n",
                        tmpname, dbp->type, dbp, tid, rc, dbp->flags);
                } while ((tid == NULL) && iter++ < 100 &&
                         rc == DB_LOCK_DEADLOCK);

                if (rc != 0) {
                    if (rc == DB_LOCK_DEADLOCK) {
                        logmsg(LOGMSG_FATAL, "deadlock in opening %s\n", tmpname);
                        exit(1);
                    }

                    print(bdb_state, "open_dbs: cannot open %s: %d %s\n",
                          tmpname, rc, db_strerror(rc));
                    rc = dbp->close(dbp, 0);
                    if (0 != rc)
                        logmsg(LOGMSG_ERROR, "DB->close(%s) failed: rc=%d %s\n",
                                tmpname, rc, db_strerror(rc));
                    if (tid)
                        tid->abort(tid);
                    return -1;
                }

                rc = dbp->get_pagesize(dbp, &x);
                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "unable to get pagesize for %s: %d %s\n",
                            tmpname, rc, db_strerror(rc));
                    exit(1);
                }

                bdb_state->dbp_data[dtanum][strnum] = dbp;
            }

            /* Don't print this trace during schemachange */
            if (!get_schema_change_in_progress(__func__, __LINE__)) {
                int calc_pgsz = calc_pagesize(bdb_state->attr->pagesizedta, bdb_state->lrl);
                if (calc_pgsz > x) {
                    logmsg(LOGMSG_WARN, "%s: Warning: Table %s has non-optimal page size. "
                           " Current: %u Optimal: %u\n",
                           __func__, bdb_state->name, x, calc_pgsz);
                }
            }
        }
    }
    if (bdbtype == BDBTYPE_QUEUEDB) {
        int max_qdb_dtanum = create ? 1 : BDB_QUEUEDB_MAX_FILES;
        assert(!create || (flags == BDB_OPEN_NONE));
        assert(BDB_QUEUEDB_MAX_FILES == 2); // TODO: Hard-coded for now.
        assert(max_qdb_dtanum >= 1);
        assert(max_qdb_dtanum <= BDB_QUEUEDB_MAX_FILES);
        for (int dtanum = 0; dtanum < max_qdb_dtanum; dtanum++) {
            if (create) {
                if ((rc = form_queuedb_name(bdb_state, &tran, dtanum, 1,
                                            tmpname, sizeof(tmpname)))) {
                    if (tid) tid->abort(tid);
                    return rc;
                }
                char new[PATH_MAX];
                print(bdb_state, "deleting %s\n", bdb_trans(tmpname, new));
                unlink(bdb_trans(tmpname, new));
            } else {
                unsigned long long old_qdb_file_ver;
                if (should_stop_looking_for_queuedb_files(bdb_state, &tran,
                                                          dtanum,
                                                          &old_qdb_file_ver)) {
                    break;
                }
                form_queuedb_name_int(
                    bdb_state, tmpname, sizeof(tmpname), old_qdb_file_ver
                );
            }
            DB *dbp;
            rc = db_create(&dbp, bdb_state->dbenv, 0);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "db_create %s: %s\n", tmpname,
                       db_strerror(rc));
                exit(1);
            }
            set_some_flags(bdb_state, dbp, tmpname);
            if (bdb_state->pagesize_override > 0) {
                pagesize = bdb_state->pagesize_override;
            } else {
                pagesize = bdb_state->attr->pagesizedta;
            }
            rc = dbp->set_pagesize(dbp, pagesize);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "unable to set pagesize on qdb to %d\n",
                       pagesize);
            }
            int qdb_type = dta_type;
            u_int32_t qdb_flags = db_flags;
            if ((iammaster) && (dtanum > 0) &&
                (flags & BDB_OPEN_ADD_QDB_FILE)) {
                logmsg(LOGMSG_DEBUG,
                       "open_dbs: file %s (%d) with CREATE flag\n",
                       tmpname, dtanum);
                qdb_type = DB_BTREE;
                qdb_flags |= DB_CREATE;
            }
            print(bdb_state, "opening %s ([%d])\n", tmpname, dtanum);
            rc = dbp->open(dbp, tid, tmpname, NULL, qdb_type, qdb_flags,
                           db_mode);
            logmsg(
                LOGMSG_DEBUG,
                "dbp->open %s type=%d dbp=%p txn=%p rc %d flags=0x%X\n",
                tmpname, dbp->type, dbp, tid, rc, dbp->flags);
            if (rc != 0) {
                if (rc == DB_LOCK_DEADLOCK) {
                    logmsg(LOGMSG_FATAL, "deadlock in open\n");
                    exit(1);
                }
                print(bdb_state, "open_dbs: cannot open %s: %d %s\n",
                      tmpname, rc, db_strerror(rc));
                rc = dbp->close(dbp, 0);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR,
                           "bdp_dta->close(%s) failed: rc=%d %s\n",
                           tmpname, rc, db_strerror(rc));
                }
                if (tid) tid->abort(tid);
                return -1;
            }
            rc = dbp->get_pagesize(dbp, &x);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "unable to get pagesize for dta\n");
                exit(1);
            }
            bdb_state->dbp_data[dtanum][0] = dbp;
        }
    }
    if (bdbtype == BDBTYPE_QUEUE || bdbtype == BDBTYPE_LITE) {
        switch (bdbtype) {
        case BDBTYPE_QUEUE:
            snprintf(tmpname, sizeof(tmpname), "XXX.%s.queue", bdb_state->name);
            break;
        case BDBTYPE_LITE:
            snprintf(tmpname, sizeof(tmpname), "XXX.%s.dta", bdb_state->name);
            break;
        default:
            break;
        }

        if (create) {
            char new[PATH_MAX];
            print(bdb_state, "deleting %s\n", bdb_trans(tmpname, new));
            unlink(bdb_trans(tmpname, new));
        }

        DB *dbp;
        rc = db_create(&dbp, bdb_state->dbenv, 0);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "db_create: %s\n", db_strerror(rc));
            exit(1);
        }

        set_some_flags(bdb_state, dbp, tmpname);

        if (bdb_state->pagesize_override > 0)
            pagesize = bdb_state->pagesize_override;
        else
            pagesize = bdb_state->attr->pagesizedta;

        if (gbl_is_physical_replicant && physrep_ignore_table(bdb_state->name)) {
            char new[PATH_MAX];
            print(bdb_state, "truncating ignored queue %s\n", bdb_trans(tmpname, new));
            rc = truncate(bdb_trans(tmpname, new), pagesize * 2);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "truncate %s error %d\n", bdb_trans(tmpname, new), errno);
            }
        }

        rc = dbp->set_pagesize(dbp, pagesize);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "unable to set pagesize on dta to %d\n", pagesize);
        }

        if (bdbtype == BDBTYPE_QUEUE) {
            int recsize = bdb_state->queue_item_sz;
            int pages = (16 * 1024 * 1024) / pagesize;
            rc = dbp->set_q_extentsize(dbp, pages);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "unable to set queue extent size to %d\n",
                        pages);
            }

            if (recsize > pagesize - QUEUE_PAGE_HEADER_SZ)
                recsize = pagesize - QUEUE_PAGE_HEADER_SZ;

            rc = dbp->set_re_len(dbp, recsize);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "unable to set record length to %d\n", recsize);
            }
        }

        print(bdb_state, "opening %s\n", tmpname);
        rc = dbp->open(dbp, tid, tmpname, NULL, dta_type, db_flags, db_mode);
        logmsg(
            LOGMSG_DEBUG,
            "dbp->open %s type=%d dbp=%p txn=%p rc %d flags=0x%X\n",
            tmpname, dbp->type, dbp, tid, rc, dbp->flags);
        if (rc != 0) {
            if (rc == DB_LOCK_DEADLOCK) {
                logmsg(LOGMSG_FATAL, "deadlock in open\n");
                exit(1);
            }

            print(bdb_state, "open_dbs: cannot open %s: %d %s\n", tmpname, rc,
                  db_strerror(rc));
            rc = dbp->close(dbp, 0);
            if (rc != 0)
                logmsg(LOGMSG_ERROR, "bdp_dta->close(%s) failed: rc=%d %s\n",
                        tmpname, rc, db_strerror(rc));

            if (tid)
                tid->abort(tid);
            return -1;
        }

        rc = dbp->get_pagesize(dbp, &x);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "unable to get pagesize for dta\n");
            exit(1);
        }

        if (bdbtype == BDBTYPE_QUEUE) {
            u_int32_t sz;
            rc = dbp->get_re_len(dbp, &sz);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "unable to get record size for queue\n");
                exit(1);
            }
            if (sz != bdb_state->queue_item_sz) {
                print(bdb_state,
                      "warning:  queue has item size %d, expected %d\n",
                      (int)sz, (int)bdb_state->queue_item_sz);
            }
            bdb_state->queue_item_sz = (size_t)sz;
        }
        bdb_state->dbp_data[0][0] = dbp;
    }

    if (bdbtype == BDBTYPE_TABLE) {
        /* set up the .ixN files */
        for (i = 0; i < bdb_state->numix; i++) {
            form_indexfile_name(bdb_state, tid, i, tmpname, sizeof(tmpname));

            if (create) {
                char new[PATH_MAX];

                print(bdb_state, "deleting %s\n", bdb_trans(tmpname, new));
                unlink(bdb_trans(tmpname, new));
            }

            /* Give indicies a 50% priority boost in the bufferpool. */
            if (bdb_state->attr->index_priority_boost)
                idx_flags = DB_INDEX_CREATE;

            rc =
                db_create(&(bdb_state->dbp_ix[i]), bdb_state->dbenv, idx_flags);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "db_create: %s\n", db_strerror(rc));
                exit(1);
            }

            /* turn on recnums if we were told to */
            if (bdb_state->ixrecnum[i]) {
                bdb_state->have_recnums = 1;

                /*fprintf(stderr, "turning on recnums\n");*/
                rc = bdb_state->dbp_ix[i]->set_flags(bdb_state->dbp_ix[i],
                                                     DB_RECNUM);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "couldnt set recnum mode\n");
                    if (tid)
                        tid->abort(tid);
                    return -1;
                }
            }

            set_some_flags(bdb_state, bdb_state->dbp_ix[i], tmpname);

            pagesize = bdb_state->attr->pagesizeix;

            /* this defaults to 4k, so assume if they went out of their
               way to override this in the lrl file, we're gonna listen */
            if (pagesize == 4096) {
                /* for datacopy indexes, use a potentially larger pagesize */
                if (bdb_state->ixdta[i]) {
                    int datacopy_size = bdb_state->ixdtalen[i] > 0 ? bdb_state->ixdtalen[i] : bdb_state->lrl;
                    pagesize =
                        calc_pagesize(bdb_state->attr->pagesizeix, datacopy_size + bdb_state->ixlen[i]);
                /*else if (bdb_state->ixcollattr[i])  ignore this for now */
                } else
                    pagesize = calc_pagesize(bdb_state->attr->pagesizeix, bdb_state->ixlen[i]);
            }

            /* get page sizes from the llmeta table if there */
            if (bdb_have_llmeta()) {
                int rc;
                int bdberr;
                int llpagesize;

                rc = bdb_get_pagesize_allindex(&tran, &llpagesize, &bdberr);

                if ((rc == 0) && (bdberr == 0)) {
                    if (llpagesize)
                        pagesize = llpagesize;
                }

                rc = bdb_get_pagesize_index(bdb_state, &tran, &llpagesize,
                                            &bdberr);

                if ((rc == 0) && (bdberr == 0)) {
                    if (llpagesize)
                        pagesize = llpagesize;
                }
            }

            rc = bdb_state->dbp_ix[i]->set_pagesize(bdb_state->dbp_ix[i],
                                                    pagesize);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "unable to set pagesize on ix %d to %d\n", i,
                        pagesize);
            }

            if (gbl_is_physical_replicant && physrep_ignore_table(bdb_state->name)) {
                char new[PATH_MAX];
                print(bdb_state, "truncating ignored queue %s\n", bdb_trans(tmpname, new));
                rc = truncate(bdb_trans(tmpname, new), pagesize * 2);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "truncate %s error %d\n", bdb_trans(tmpname, new), errno);
                }
            }

            print(bdb_state, "opening %s ([%d])\n", tmpname, i);
            if (bdb_state->attr->page_compact_indexes /* compact index */
                && !bdb_state->ixrecnum[i]            /* not recnum */
                && strncasecmp(bdb_state->name, "sqlite_stat1", 11) != 0)
                db_flags |= DB_OLCOMPACT;
            rc = bdb_state->dbp_ix[i]->open(bdb_state->dbp_ix[i], tid, tmpname,
                                            NULL, DB_BTREE, db_flags, db_mode);
            if (rc != 0) {
                if (rc == DB_LOCK_DEADLOCK) {
                    logmsg(LOGMSG_FATAL, "deadlock in open\n");
                    exit(1);
                }

                bdb_state->dbp_ix[i]->err(bdb_state->dbp_ix[i], rc, "%s",
                                          tmpname);
                rc = bdb_state->dbp_ix[i]->close(bdb_state->dbp_ix[i], 0);
                logmsg(LOGMSG_ERROR, "close ix=%d name=%s failed rc=%d\n", i,
                        tmpname, rc);
                logmsg(LOGMSG_ERROR, "couldnt open ix db\n");

                if (tid)
                    tid->abort(tid);
                return -1;
            }

            rc = bdb_state->dbp_ix[i]->get_pagesize(bdb_state->dbp_ix[i], &x);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "unable to get pagesize for ix %d\n", i);
                exit(1);
            }
        }
    } /* end of non-open-lite block */

    if (tmp_tid) {
        rc = tid->commit(tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "open_dbs: commit %d\n", rc);
            return -1;
        }
    }

    /* For dtastripe find our highest genid so we can set the cmp context
     * appropriately.  This is a bug fix and an optimisation since it faults
     * in the insertion end of the btree, woohoo! */
    if (bdbtype == BDBTYPE_TABLE) {
        unsigned long long maxgenid = 0;
        int stripe;
        unsigned long long master_cmpcontext;

        for (stripe = 0; stripe < bdb_state->attr->dtastripe; stripe++) {
            DBC *dbcp;
            DB *dbp;
            DBT dbt_key, dbt_data;
            unsigned long long genid;

            dbp = bdb_state->dbp_data[0][stripe];

            if (tmp_tid == 0) {
                assert(tid != 0);
                rc = dbp->cursor(dbp, tid, &dbcp, 0);
            } else {
                rc = dbp->cursor(dbp, NULL, &dbcp, 0);
            }

            if (rc != 0) {
                logmsg(LOGMSG_ERROR, 
                       "open_dbs: %s: cannot open cursor on stripe %d: %d %s\n",
                       bdb_state->name, stripe, rc, db_strerror(rc));
            } else {
                /* key will contain genid.  don't retrieve any data. */
                bzero(&dbt_key, sizeof(dbt_key));
                bzero(&dbt_data, sizeof(dbt_data));
                dbt_key.size = sizeof(genid);
                dbt_key.ulen = sizeof(genid);
                dbt_key.data = &genid;
                dbt_key.flags = DB_DBT_USERMEM;
                dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

                rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_LAST);
                if (rc == 0) {
                    genid = bdb_mask_stripe(bdb_state, genid);
                    if (bdb_cmp_genids(genid, maxgenid) > 0)
                        maxgenid = genid;
                } else if (rc != DB_NOTFOUND) {
                    logmsg(LOGMSG_ERROR, "open_dbs: %s: cannot find last genid on "
                                    "stripe %d: %d %s\n",
                            bdb_state->name, stripe, rc, db_strerror(rc));
                }
                dbcp->c_close(dbcp);
            }
        }

        print(bdb_state, "open_dbs: %s: max genid is 0x%llx\n", bdb_state->name,
              maxgenid);

        /* Set compare context to be used in comparisons.  This needs to be
         * bigger than the max genid because otherwise the very last record
         * would never be found... */
        bdb_state->master_cmpcontext = bdb_increment_slot(bdb_state, maxgenid);
        master_cmpcontext = bdb_state->master_cmpcontext;

        if (maxgenid)
            set_gblcontext(bdb_state, master_cmpcontext);
    }

    return 0;
}

static int bdb_create_stripes_int(bdb_state_type *bdb_state, tran_type *tran,
                                  int newdtastripe, int newblobstripe,
                                  int *bdberr)
{
    int dtanum, strnum;
    int numdtafiles;
    int db_mode = 0666;
    int db_flags = DB_THREAD | DB_CREATE;
    int dta_type = DB_BTREE;
    int rc, ii;
    int created_tid = 0;
    DB_TXN *tid = NULL;
    int dbp_count = 0;
    DB *dbp_array[256];

    /* Only affects blob files if we have, or are converting to, blobstripe. */
    if (newblobstripe || bdb_state->attr->blobstripe)
        numdtafiles = bdb_state->numdtafiles;
    else
        numdtafiles = 1;

    if (tran) {
        tid = tran->tid;
    } else {
        created_tid = 1;
        rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR,
                   "bdb_create_stripes_int: begin transaction failed\n");
            return -1;
        }
    }

    for (dtanum = 0; dtanum < numdtafiles; dtanum++) {
        int numstripes = bdb_get_datafile_num_files(bdb_state, dtanum);

        /* Add the extra stripes. */
        for (strnum = numstripes; strnum < newdtastripe; strnum++) {
            char tmpname[PATH_MAX];
            char new[PATH_MAX];
            int pagesize;
            DB *dbp = NULL;

            /* For the blob files never do anything with the first file.
             * If we are converting to blob stripe, it will get renamed later
             * on. */
            if (dtanum > 0 && strnum == 0)
                continue;

            /* Form file name */
            form_file_name(bdb_state, tid, 1 /*is_data_file*/, dtanum,
                           1 /*isstriped*/, strnum, tmpname, sizeof(tmpname));

            unlink(bdb_trans(tmpname, new));

            rc = db_create(&dbp, bdb_state->dbenv, 0);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "bdb_create_stripes_int: db_create %s: %s\n",
                        tmpname, db_strerror(rc));
                return -1;
            }

            set_some_flags(bdb_state, dbp, tmpname);

            if (bdb_state->pagesize_override > 0)
                pagesize = bdb_state->pagesize_override;
            else
                pagesize = bdb_state->attr->pagesizedta;
            rc = dbp->set_pagesize(dbp, pagesize);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "bdb_create_stripes_int: unable to set "
                                "pagesize on %s to %d\n",
                        tmpname, pagesize);
            }

            print(bdb_state, "opening %s ([%d][%d])\n", tmpname,
                  dtanum, strnum);
            if (dtanum == 0 /* not blob */
                && strncasecmp(bdb_state->name, "sqlite_stat", 11) != 0)
                /* don't compact sqlite_stat tables */
                db_flags |= DB_OLCOMPACT;
            rc =
                dbp->open(dbp, tid, tmpname, NULL, dta_type, db_flags, db_mode);
            if (rc != 0) {
                if (rc == DB_LOCK_DEADLOCK) {
                    logmsg(LOGMSG_FATAL,
                            "bdb_create_stripes_int: deadlock in opening %s\n",
                            tmpname);
                    exit(1);
                }

                logmsg(LOGMSG_ERROR, "bdb_create_stripes_int: cannot open %s: %d %s\n",
                        tmpname, rc, db_strerror(rc));
                rc = dbp->close(dbp, 0);
                if (0 != rc)
                    logmsg(LOGMSG_ERROR, "DB->close(%s) failed: rc=%d %s\n", tmpname,
                            rc, db_strerror(rc));
                if (tid && created_tid)
                    tid->abort(tid);

                return -1;
            }

           logmsg(LOGMSG_INFO, "Created %s\n", tmpname);

            dbp_array[dbp_count++] = dbp;
            /* And close it again */
        }
    }

    if (created_tid) {
        rc = tid->commit(tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "bdb_create_stripes_int: commit: %d %s\n", rc,
                   db_strerror(rc));
            return -1;
        }
    }

    /* Now go and close all the tables. */
    for (ii = 0; ii < dbp_count; ii++) {
        rc = dbp_array[ii]->close(dbp_array[ii], 0);
        if (0 != rc)
            logmsg(LOGMSG_ERROR,
                    "bdb_create_stripes_int: DB->close #%d failed: rc=%d %s\n",
                    ii, rc, db_strerror(rc));
    }

    return 0;
}

int bdb_create_stripes_tran(bdb_state_type *bdb_state, tran_type *tran,
                            int newdtastripe, int newblobstripe, int *bdberr)
{
    int rc;
    BDB_READLOCK("bdb_create_stripes");
    rc = bdb_create_stripes_int(bdb_state, tran, newdtastripe, newblobstripe,
                                bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_create_stripes(bdb_state_type *bdb_state, int newdtastripe,
                       int newblobstripe, int *bdberr)
{
    int rc;
    rc = bdb_create_stripes_tran(bdb_state, NULL, newdtastripe, newblobstripe,
                                 bdberr);
    return rc;
}

static void fix_context(bdb_state_type *bdb_state)
{
    unsigned long long correct_context = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (get_gblcontext(bdb_state) == -1ULL) {
        logmsg(LOGMSG_ERROR, "%s: detected BAD context %llu, fixing\n",
               __func__, get_gblcontext(bdb_state));

        /* sleep 1 sec to avoid dups, which are broken now! */
        sleep(1);

        correct_context = bdb_get_cmp_context(bdb_state);
        set_gblcontext(bdb_state, correct_context);

        logmsg(LOGMSG_ERROR, "%s: FIXING context to %llx\n", __func__,
               correct_context);
    }
}

/*
   this is essentially the old "downgrade" code.  we no longer close/open
   files when we downgrade, so we do it in this routine, which we call
   only when we need to.  after calling this file, we will have downgraded
   ourselves.  if we used to be master, thats ok, we'll cause an election
   after this anyway, since we dont know who the master is
   (bdb_state->repinfo->master_host = db_eid_invalid)
   */
static int bdb_reopen_int(bdb_state_type *bdb_state)
{
    int rc;
    int outrc;
    bdb_state_type *child;
    int i;
    DB_TXN *tid;

    BDB_READLOCK("bdb_reopen_int");

    outrc = 0;

    if (!bdb_state->repinfo->upgrade_allowed) {
        return 0;
    }

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bdb_state->read_write = 0;

    rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "bdb_reopen_int: begin transaction failed\n");
        exit(1);
    }

    if (!bdb_state->envonly) {
        /* close all of our databases.  doesn't fail */
        rc = close_dbs(bdb_state);

        /* fprintf(stderr, "back from close_dbs\n"); */

        /* now reopen them as a client */
        rc = open_dbs(bdb_state, 0, 1, 0, tid, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "upgrade: open_dbs as client failed\n");
            outrc = 1;
            goto end;
        }
        bdb_state->isopen = 1;
    }

    /* now do it for all of our children */
    bdb_lock_children_lock(bdb_state);
    for (i = 0; i < bdb_state->numchildren; i++) {
        child = bdb_state->children[i];
        if (child) {

            child->read_write = 0;

            /* close all of our databases.  doesn't fail */
            rc = close_dbs(child);

            /* fprintf(stderr, "back from close_dbs\n"); */

            /* now reopen them as a client */
            rc = open_dbs(child, 0, 1, 0, tid, 0);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "upgrade: open_dbs as client failed\n");
                outrc = 1;
                goto end;
            }
            child->isopen = 1;
        }
    }
    bdb_unlock_children_lock(bdb_state);

    /* fprintf(stderr, "back from open_dbs\n"); */

    rc = tid->commit(tid, 0);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "bdb_reopen_int: commit transaction failed\n");
        exit(1);
    }

    /* now become a client of the replication group */
    logmsg(LOGMSG_USER, "%s line %d calling rep_start as client with egen 0\n",
           __func__, __LINE__);

    rc = bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, 0, DB_REP_CLIENT);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "rep_start as client failed\n");
        outrc = 1;
    }

    logmsg(LOGMSG_DEBUG, "back from rep_start\n");

end:

    BDB_RELLOCK();

    return outrc;
}

static void whoismaster_rtn(bdb_state_type *bdb_state, int sc_clear)
{
    if (!bdb_state->callback->whoismaster_rtn) return;
    bdb_state->callback->whoismaster_rtn(bdb_state, bdb_state->repinfo->master_host, sc_clear); /* new_master_callback */
}

void bdb_setmaster(bdb_state_type *bdb_state, char *host)
{
    BDB_READLOCK("bdb_setmaster");

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    set_repinfo_master_host(bdb_state, host, __func__, __LINE__);

    BDB_RELLOCK();

    whoismaster_rtn(bdb_state, 0);
}

void bdb_set_read_only(bdb_state_type *bdb_state)
{
    bdb_state_type *child;
    int i;

    if (!bdb_state->repinfo->upgrade_allowed) {
        logmsg(LOGMSG_DEBUG,
               "%s: not allowed (bdb_open has not completed yet)\n", __func__);
        return;
    }

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bdb_state->read_write = 0;

    bdb_lock_children_lock(bdb_state);
    for (i = 0; i < bdb_state->numchildren; i++) {
        child = bdb_state->children[i];
        if (child) {
            child->read_write = 0;
        }
    }
    bdb_unlock_children_lock(bdb_state);
}

static int bdb_downgrade_int(bdb_state_type *bdb_state, int noelect,
                             int *downgraded)
{
    int rc;
    int outrc;

    outrc = 0;
    if (downgraded)
        *downgraded = 0;

    if (!bdb_state->repinfo->upgrade_allowed) {
        logmsg(LOGMSG_DEBUG, "bdb_downgrade: not allowed (bdb_open has not "
                             "completed yet)\n");
        return 0;
    }

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bdb_set_read_only(bdb_state);

    /* now become a client of the replication group */
    logmsg(LOGMSG_USER, "%s line %d calling rep_start as client with egen 0\n",
           __func__, __LINE__);
    rc = bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, 0, DB_REP_CLIENT);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "rep_start as client failed\n");
        outrc = 1;
    } else {
        print(bdb_state, "%s: started rep as CLIENT\n", __func__);
    }

    logmsg(LOGMSG_DEBUG, "back from rep_start\n");

    if (downgraded)
        *downgraded = 1;

    if (!noelect)
        call_for_election_and_lose(bdb_state, __func__, __LINE__);

    logmsg(LOGMSG_ERROR, "%s returning\n", __func__);
    return outrc;
}

void defer_commits_for_upgrade(bdb_state_type *bdb_state, const char *host,
                               const char *func);

static int bdb_upgrade_int(bdb_state_type *bdb_state, uint32_t newgen,
                           int *upgraded)
{
    int rc;
    int outrc;
    bdb_state_type *child;
    int i;

    osql_repository_cancelall();

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    outrc = 0;

    if (upgraded)
        *upgraded = 0;

    if (!bdb_state->repinfo->upgrade_allowed) {
        /* this is how this sicko works:
           environment is not opened yet, it actually waits for a master to be
           set and, yes, I am the master and this function has to set it
           Since the environment is not quite open, but we know
           that initializion during startup provide the same var reset, I can
           just inform Berkeley we're the big guy and return
           (rowlocks probably needs to be revised).
           Calling rep_start generates a broadcast, which we intercept and
           set the master ! which unlocks the bdb_open_env or open_bdb_env,
           and db comes up finally.
        */
        logmsg(LOGMSG_USER,
               "%s line %d calling rep_start as master with egen 0\n", __func__,
               __LINE__);
        rc = bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, newgen,
                                         DB_REP_MASTER);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "rep_start failed rc %d\n", rc);
            return -1;
        } else {
            /* special case upgrade codepath to get this set faster */

            set_repinfo_master_host(bdb_state, bdb_state->repinfo->myhost,
                                    __func__, __LINE__);
        }

        defer_commits_for_upgrade(bdb_state, 0, __func__);

        if (upgraded)
            *upgraded = 1;

        return 0;
    }

    /* If this node is rtcpu'd off don't upgrade. */
    if ((bdb_state->callback->nodeup_rtn) &&
        !(bdb_state->callback->nodeup_rtn(bdb_state,
                                          bdb_state->repinfo->myhost))) {
        /* Make sure that we will allow ourselves to upgrade, and that we won't
           transfer our mastership immediately. */
        if (bdb_state->attr->allow_offline_upgrades) {
            logmsg(LOGMSG_ERROR, "%s: rtcpu'd but allowing an upgrade because "
                            "'allow_offline_upgrades' is true.\n",
                    __func__);
        } else {
            logmsg(LOGMSG_WARN, "%s: not upgrading because I am rtcpu'd.\n",
                    __func__);
            return -1;
        }
    }

    /* patch for context */
    fix_context(bdb_state);

    bdb_state->read_write = 1;

    bdb_lock_children_lock(bdb_state);
    for (i = 0; i < bdb_state->numchildren; i++) {
        child = bdb_state->children[i];
        if (child) {
            child->read_write = 1;
        }
    }
    bdb_unlock_children_lock(bdb_state);
    logmsg(LOGMSG_USER, "%s line %d calling rep_start as master with egen %d\n",
           __func__, __LINE__, newgen);
    rc = bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, newgen,
                                     DB_REP_MASTER);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "rep_start failed rc %d\n", rc);
        return 1;
    } else {
        /* special case upgrade codepath to get this set faster */
        set_repinfo_master_host(bdb_state, bdb_state->repinfo->myhost, __func__,
                                __LINE__);
    }

    defer_commits_for_upgrade(bdb_state, 0, __func__);

    /* notify the user that we are the master */
    whoismaster_rtn(bdb_state, 1);

    /* master cannot be incoherent, that makes no sense.
     *
     * Should be after the newmaster callback: the qtrap looks at
     * thedb->master (which is set in the above newmaster callback) to
     * determine whether we should ignore a NOTCOHERENT2 message.
     */
    if (bdb_state->not_coherent) {
        logmsg(LOGMSG_INFO, "%s: clearing not_coherent due to upgrade\n", __func__);
        bdb_state->not_coherent = 0;
    }

    bdb_state->caught_up = 1;

    if (upgraded)
        *upgraded = 1;

    if (gbl_rowlocks) {
        /* run master version of logical recovery */
        rc = bdb_run_logical_recovery(bdb_state, 0);
        if (rc) {
           logmsg(LOGMSG_ERROR, "%s:%d bdb_run_logical_recovery rc %d\n", __FILE__, __LINE__,
                   rc);
            outrc = rc;
        }
    }

    return outrc;
}

enum { UPGRADE = 1, DOWNGRADE = 2, DOWNGRADE_NOELECT = 3, REOPEN = 4 };

void *dummy_add_thread(void *arg);
void bdb_all_incoherent(bdb_state_type *bdb_state);

struct bdblock_state {
    bdb_state_type *bdb_state;
    pthread_mutex_t lk;
};

static void *bdb_abort_prepared_thd(void *arg)
{
    struct bdblock_state *b = (struct bdblock_state *)arg;
    bdb_state_type *bdb_state = (bdb_state_type *)b->bdb_state;

    /* Stop when we can acquire mutex */
    while (pthread_mutex_trylock(&b->lk) != 0) {
        if (bdb_lock_desired(bdb_state)) {
            logmsg(LOGMSG_INFO, "%s aborting waiters on prepared txns\n", __func__);
            bdb_state->dbenv->txn_abort_prepared_waiters(bdb_state->dbenv);
        }
        poll(0, 0, 100);
    }
    free(b);
    return NULL;
}

/* Unresolved prepared transactions hold locks.  Other transactions can block on
 * these locks.  We've been asked to downgrade, so we need to cancel these
 * blocked transactions so that we can acquire the bdb-lock.
 *
 * TODO: think about how to downgrade normal prepared-transactions on the master */
static struct bdblock_state *abort_prepared_waiters(bdb_state_type *bdb_state)
{
    struct bdblock_state *b = calloc(sizeof(struct bdblock_state), 1);
    b->bdb_state = bdb_state;
    Pthread_mutex_init(&b->lk, 0);
    Pthread_mutex_lock(&b->lk);
    pthread_t abort_prepared_td;
    pthread_attr_t thd_attr;
    Pthread_attr_init(&thd_attr);
    Pthread_attr_setdetachstate(&thd_attr, PTHREAD_CREATE_DETACHED);
    Pthread_create(&abort_prepared_td, &thd_attr, bdb_abort_prepared_thd, b);
    return b;
}

static int bdb_upgrade_downgrade_reopen_wrap(bdb_state_type *bdb_state, int op,
                                             int timeout, uint32_t newgen,
                                             int *done)
{
    int rc = 0;
    struct bdblock_state *lockstate = NULL;
    char *lock_str;

    if (done) {
        *done = 0;
    }

    if (op != UPGRADE) {
        wait_for_sc_to_stop("downgrade", __func__, __LINE__);
        bdb_set_read_only(bdb_state);
    }

    watchdog_set_alarm(timeout);

    if (op == DOWNGRADE || op == DOWNGRADE_NOELECT) {
        lockstate = abort_prepared_waiters(bdb_state);
    }

    switch (op) {
    case DOWNGRADE:
        lock_str = "downgrade";
        BDB_WRITELOCK(lock_str);
        break;
    case DOWNGRADE_NOELECT:
        lock_str = "downgrade_noelect";
        BDB_WRITELOCK(lock_str);
        break;
    case UPGRADE:
        /* no need to stop threads to upgrade;
          UPDATE: this generates more pain because:
          - sometimes I get two upgrade processes concurrently, both
            reader and watcher both upgrading the node;
          - rep_start gets a WRITE lock ANYWAY, getting everybody out
          - rep_start WRITE_LOCK releases the lock on busy lock,
            and we can have a concurrent downgrade which gets the lock
            and makes setting the read_write variable even
            more important (and hard to get right)
            SO, back to WRITELOCK here
        */
        lock_str = "upgrade";
        BDB_WRITELOCK(lock_str);
        bdb_all_incoherent(bdb_state);
        break;
    case REOPEN:
        lock_str = "reopen";
        BDB_WRITELOCK(lock_str);
        break;
    default:
        logmsg(LOGMSG_FATAL, "%s unhandled %d\n", __func__, op);
        exit(1);
        break;
    }
    if (lockstate) {
        Pthread_mutex_unlock(&lockstate->lk);
    }

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    switch (op) {
    case DOWNGRADE:
    case DOWNGRADE_NOELECT:
    case REOPEN:

        if (op == REOPEN) {
            rc = bdb_reopen_int(bdb_state);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "bdb_reopen_int rc %d\n", rc);
                exit(1);
            }
        }

        logmsg(LOGMSG_DEBUG, "calling bdb_downgrade_int\n");
        if (op == DOWNGRADE)
            rc = bdb_downgrade_int(bdb_state, 0, done);
        else {
            rc = bdb_downgrade_int(bdb_state, 1, done);
            if (op == DOWNGRADE_NOELECT) {
                assert(bdb_state->parent == NULL);
                if (bdb_state->repinfo->master_host ==
                    bdb_state->repinfo->myhost) {
                    /* we need the watcher thread to kick periodical elections
                       to get us a new master
                       this handles the cluster split case */
                    set_repinfo_master_host(bdb_state, db_eid_invalid, __func__,
                                            __LINE__);
                }
            }
        }
        logmsg(LOGMSG_DEBUG, "back from bdb_downgrade_int\n");
        break;

    case UPGRADE:
        logmsg(LOGMSG_DEBUG, "calling bdb_upgrade_int\n");
        rc = bdb_upgrade_int(bdb_state, newgen, done);
        logmsg(LOGMSG_DEBUG, "back from bdb_upgrade_int\n");

        {
            pthread_t tid;

            /* schedule a dummy add */
            pthread_create(&tid, &(bdb_state->pthread_attr_detach),
                           dummy_add_thread, bdb_state);
        }

        break;
    }

    whoismaster_rtn(bdb_state, 1);
    allow_sc_to_run();
    BDB_RELLOCK();

    watchdog_cancel_alarm();

    return rc;
}

int bdb_upgrade(bdb_state_type *bdb_state, uint32_t newgen, int *done)
{
    int i;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->exiting)
        return 0;

    logmsg(LOGMSG_DEBUG, "%s:%d %s set file = 0\n", __FILE__, __LINE__, __func__);
    if (gbl_set_seqnum_trace) {
        logmsg(LOGMSG_USER, "%s line %d setting all seqnums to 0\n", __func__,
               __LINE__);
    }
    for (i = 0; i < MAXNODES; i++) {
        bdb_state->seqnum_info->seqnums[i].lsn.file = 0;
    }

    return bdb_upgrade_downgrade_reopen_wrap(bdb_state, UPGRADE, 30, newgen,
                                             done);
}

int bdb_downgrade(bdb_state_type *bdb_state, uint32_t newgen, int *done)
{
    return bdb_upgrade_downgrade_reopen_wrap(bdb_state, DOWNGRADE, 5, newgen,
                                             done);
}

int bdb_downgrade_noelect(bdb_state_type *bdb_state)
{
    return bdb_upgrade_downgrade_reopen_wrap(bdb_state, DOWNGRADE_NOELECT, 5, 0,
                                             NULL);
}

/* not intended to be called by anyone but elect thread */
int bdb_reopen_inline(bdb_state_type *bdb_state)
{
    return bdb_upgrade_downgrade_reopen_wrap(bdb_state, REOPEN, 5, 0, NULL);
}

extern pthread_key_t lockmgr_key;

static void run_once(void)
{
    Pthread_key_create(&lockmgr_key, NULL);

    Pthread_key_create(&bdb_key, NULL);

    Pthread_key_create(&lock_key, bdb_lock_destructor);
}

static void deadlock_happened(struct berkdb_deadlock_info *deadlock_info)
{
    if (debug_switch_verbose_deadlocks_log())
        ctrace("deadlk %u %" PRIxPTR "\n", deadlock_info->lid, (intptr_t)pthread_self());
}

/* clone clone_bdb_state and then copy over the data file pointers from
 * data_files_bdb_state.
 * all the memory is still owned by clone_bdb_state and data_files_bdb_state so
 * only the state object itself needs to be free()'d */
bdb_state_type *bdb_clone_handle_with_other_data_files(
    const bdb_state_type *clone_bdb_state,
    const bdb_state_type *data_files_bdb_state)
{
    int strnum;
    int maxstrnum;
    bdb_state_type *new_bdb_state;
    if (!(new_bdb_state = malloc(sizeof(bdb_state_type))))
        return NULL;

    /* clone all data/pointers */
    *new_bdb_state = *clone_bdb_state;

    /* overwrite the data file pointers */
    maxstrnum = (data_files_bdb_state->attr->dtastripe)
                    ? data_files_bdb_state->attr->dtastripe
                    : 1;
    for (strnum = 0; strnum < maxstrnum; ++strnum)
        new_bdb_state->dbp_data[0][strnum] =
            data_files_bdb_state->dbp_data[0][strnum];

    return new_bdb_state;
}

/* clean up after bdb_clone_handle_with_other_data_files() */
void bdb_free_cloned_handle_with_other_data_files(bdb_state_type *bdb_state)
{
    free(bdb_state);
}

int bdb_is_open(bdb_state_type *bdb_state) { return bdb_state->isopen; }

int create_master_lease_thread(bdb_state_type *bdb_state)
{
    pthread_t tid;
    pthread_attr_t attr;
    Pthread_attr_init(&attr);
    Pthread_attr_setstacksize(&attr, 128 * 1024);
    pthread_create(&tid, &attr, master_lease_thread, bdb_state);
    Pthread_attr_destroy(&attr);
    return 0;
}

void create_coherency_lease_thread(bdb_state_type *bdb_state)
{
    pthread_t tid;
    pthread_attr_t attr;
    Pthread_attr_init(&attr);
    Pthread_attr_setstacksize(&attr, 128 * 1024);
    pthread_create(&tid, &attr, coherency_lease_thread, bdb_state);
    Pthread_attr_destroy(&attr);
}

static comdb2bma bdb_blobmem;
static pthread_once_t bdb_blobmem_once = PTHREAD_ONCE_INIT;
static void bdb_blobmem_init_once(void)
{
    bdb_blobmem = comdb2bma_create(0, gbl_blobmem_cap, "bdb/blob", NULL);
    if (bdb_blobmem == NULL) {
        logmsg(LOGMSG_FATAL, "failed creating bdb blob allocator\n");
        abort();
    }
}

static bdb_state_type *bdb_open_int(
    int envonly, const char name[], const char dir[], int lrl, short numix,
    const short ixlen[], const signed char ixdups[],
    const signed char ixrecnum[], const signed char ixdta[], const int ixdtalen[],
    const signed char ixcollattr[], const signed char ixnulls[],
    int numdtafiles, bdb_attr_type *bdb_attr, bdb_callback_type *bdb_callback,
    void *usr_ptr, netinfo_type *netinfo, int upgrade, int create, int *bdberr,
    bdb_state_type *parent_bdb_state, int pagesize_override, bdbtype_t bdbtype,
    DB_TXN *tid, int temp, char *recoverylsn, uint32_t flags)
{
    bdb_state_type *bdb_state;
    int rc;
    int i;
    int largest;
    struct stat sb;
    int iammaster;

    pthread_t dummy_tid;
    const char *tmp;
    extern unsigned gbl_blob_sz_thresh_bytes;

    pthread_once(&ONCE_LOCK, run_once);

    iammaster = 0;

    if (numix > MAXINDEX) {
        logmsg(LOGMSG_ERROR,"%s: Maximum number of indexes per table exceeded."
                            "attempted - %d, Maximum - %d\n",__func__,
                            numix, MAXINDEX);

        *bdberr = BDBERR_EXCEEDED_INDEXES;
        return NULL;
    }

    if ((bdbtype == BDBTYPE_QUEUE || bdbtype == BDBTYPE_QUEUEDB) && lrl <= 0) {
        logmsg(LOGMSG_ERROR, "bdb_open_int: bad lrl for queue %d\n", lrl);
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }
    if (bdbtype == BDBTYPE_LITE && numix != 1) {
        logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
        *bdberr = BDBERR_MISC;
        return NULL;
    }
    if ((bdbtype == BDBTYPE_QUEUE || bdbtype == BDBTYPE_QUEUEDB) &&
        numix != 0) {
        logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
        *bdberr = BDBERR_MISC;
        return NULL;
    }
    if (envonly && bdbtype != BDBTYPE_ENV) {
        logmsg(LOGMSG_ERROR, "bdb_open_int: envonly but type is not BDBTYPE_ENV\n");
        logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
        *bdberr = BDBERR_MISC;
        return NULL;
    }
    if (!envonly && bdbtype == BDBTYPE_ENV) {
        logmsg(LOGMSG_ERROR, "bdb_open_int: not envonly but type is BDBTYPE_ENV\n");
        logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
        *bdberr = BDBERR_MISC;
        return NULL;
    }

    if ((envonly && numdtafiles != 0) ||
        (!envonly && (numdtafiles > MAXDTAFILES || numdtafiles < 1))) {
        logmsg(LOGMSG_ERROR,"%s: Maximum number of blob/vutf8 fields exceeded."
                        "attempted - %d, Maximum - %d\n",__func__,
                        numdtafiles,MAXDTAFILES);
        *bdberr = BDBERR_EXCEEDED_BLOBS;
        return NULL;
    }

    bdb_state = mymalloc(sizeof(bdb_state_type));
    bzero(bdb_state, sizeof(bdb_state_type));
    bdb_state->name = strdup(name);

    /* This is a bit of a kludge.  We use the first 32 bytes of the table name
     * as a key into llmeta. This has always been a bug - this is a quick patch
     * for it for allow currently valid tablenames to be changed.  What changed?
     * get_sc_to_name used to return a full (non-truncated) table name. Now it
     * returns a truncated one.  That's because the bdb_state->name is not
     * derivable by the name string passed to this routine (get_sc_to_name used
     * to look at a global, which no longer exists.)  So this routine caps the
     * limit instead of the caller capping the limit. */
    if (strlen(name) >= 32 && bdbtype == BDBTYPE_TABLE)
        bdb_state->name[31] = 0;

    bdb_state->dir = strdup(dir);
    bdb_state->bdbtype = bdbtype;
    tmp = get_sc_to_name(name);
    if (tmp)
        bdb_state->origname = strdup(tmp);
    else
        bdb_state->origname = NULL;

    if (!parent_bdb_state) {
        if (gbl_bdblock_debug) {
            bdb_bdblock_debug_init(bdb_state);
        }

        Pthread_key_create(&(bdb_state->tid_key), NULL);

        Pthread_mutex_init(&(bdb_state->numthreads_lock), NULL);
        Pthread_mutex_init(&(bdb_state->id_lock), NULL);
        Pthread_mutex_init(&(bdb_state->gblcontext_lock), NULL);

        bdb_state->last_downgrade_time = calloc(sizeof(uint64_t), MAXNODES);
        bdb_state->master_lease = calloc(sizeof(uint64_t), MAXNODES);
        Pthread_mutex_init(&(bdb_state->master_lease_lk), NULL);

        bdb_state->coherent_state = malloc(sizeof(int) * MAXNODES);
        for (int i = 0; i < MAXNODES; i++)
            bdb_state->coherent_state[i] = STATE_COHERENT;

        Pthread_mutex_init(&(bdb_state->coherent_state_lock), NULL);

        bdb_lock_init(bdb_state);

        Pthread_mutex_init(&bdb_state->durable_lsn_lk, NULL);
    }

    /* XXX this looks wrong */
    if (!parent_bdb_state)
        bdb_thread_event(bdb_state, 1);

    Pthread_attr_init(&(bdb_state->pthread_attr_detach));

    Pthread_attr_setdetachstate(&(bdb_state->pthread_attr_detach),
                                PTHREAD_CREATE_DETACHED);
    Pthread_attr_setstacksize(&bdb_state->pthread_attr_detach, 1024 * 1024);

    if (bdbtype == BDBTYPE_TABLE || bdbtype == BDBTYPE_LITE)
        bdb_state->lrl = lrl;
    else if (bdbtype == BDBTYPE_QUEUE)
        bdb_state->queue_item_sz = lrl + sizeof(struct bdb_queue_header);
    else if (bdbtype == BDBTYPE_QUEUEDB)
        bdb_state->queue_item_sz = lrl;

    if (!parent_bdb_state) {
        /* init seqnum_info */
        bdb_state->seqnum_info = mymalloc(sizeof(seqnum_info_type));
        bzero(bdb_state->seqnum_info, sizeof(seqnum_info_type));

        bdb_state->seqnum_info->seqnums =
            mymalloc(sizeof(seqnum_type) * MAXNODES);
        bzero(bdb_state->seqnum_info->seqnums, sizeof(seqnum_type) * MAXNODES);

        bdb_state->seqnum_info->filenum = mymalloc(sizeof(int) * MAXNODES);
        bzero(bdb_state->seqnum_info->filenum, sizeof(int) * MAXNODES);
    } else {
        /* share the parent */
        bdb_state->seqnum_info = parent_bdb_state->seqnum_info;
    }

    Pthread_mutex_init(&(bdb_state->exit_lock), NULL);

    /* initialize this thing high so any findnexts that happen before we
       get a broadcast from master will not skip anything */
    bdb_state->master_cmpcontext = flibc_htonll(ULLONG_MAX);

    bdb_state->seed = 0;
    Pthread_mutex_init(&(bdb_state->seed_lock), NULL);

    if (!parent_bdb_state) {
        Pthread_mutex_init(&(bdb_state->seqnum_info->lock), NULL);
        Pthread_cond_init(&(bdb_state->seqnum_info->cond), NULL);
        bdb_state->seqnum_info->waitlist =
            calloc(MAXNODES, sizeof(wait_for_lsn_list *));
        bdb_state->seqnum_info->trackpool = pool_setalloc_init(
            sizeof(struct waiting_for_lsn), 100, malloc, free);
        bdb_state->seqnum_info->time_10seconds =
            calloc(MAXNODES, sizeof(struct averager *));
        bdb_state->seqnum_info->time_minute =
            calloc(MAXNODES, sizeof(struct averager *));
        bdb_state->seqnum_info->expected_udp_count =
            calloc(MAXNODES, sizeof(short));
        bdb_state->seqnum_info->incomming_udp_count =
            calloc(MAXNODES, sizeof(short));
        bdb_state->seqnum_info->udp_average_counter =
            calloc(MAXNODES, sizeof(short));

        for (i = 0; i < 16; i++)
            bdb_state->stripe_pool[i] = 255;
        bdb_state->stripe_pool[16] = 0;

        bdb_state->stripe_pool_start = 0;

        Pthread_key_create(&(bdb_state->seqnum_info->key), NULL);

        bdb_state->attr = bdb_attr;
        bdb_state->usr_ptr = usr_ptr;
        bdb_state->callback = bdb_callback;

        bdb_state->bdb_lock = mymalloc(sizeof(pthread_rwlock_t));
        Pthread_rwlock_init(bdb_state->bdb_lock, NULL);
        Pthread_mutex_init(&(bdb_state->children_lock), NULL);
        bdb_state->have_children_lock = 0;

    } else {
        bdb_state->parent = parent_bdb_state;

        /* we share our attributes, usrptr, and callbacks with our parent */
        bdb_state->attr = bdb_state->parent->attr;
        bdb_state->usr_ptr = bdb_state->parent->usr_ptr;
        bdb_state->callback = bdb_state->parent->callback;

        /* we share our bdb_lock with our parent. */
        bdb_state->bdb_lock = bdb_state->parent->bdb_lock;

        bdb_state->children_lock = bdb_state->parent->children_lock;
    }

    int nlen = strlen(bdb_state->name) + strlen(bdb_state->dir) + 100;
    bdb_state->txndir = mymalloc(nlen);
    bdb_state->tmpdir = mymalloc(nlen);

    bdb_state->numdtafiles = numdtafiles;
    bdb_state->numix = numix;

    if (bdb_state->numix) {
        for (i = 0; i < numix; i++) {
            if (ixlen)
                bdb_state->ixlen[i] = ixlen[i];
            else
                bdb_state->ixlen[i] = 0;

            if (ixdups)
                bdb_state->ixdups[i] = ixdups[i];
            else
                bdb_state->ixdups[i] = 0;

            if (ixrecnum)
                bdb_state->ixrecnum[i] = ixrecnum[i];
            else
                bdb_state->ixrecnum[i] = 0;

            if (ixdta)
                bdb_state->ixdta[i] = ixdta[i];
            else
                bdb_state->ixdta[i] = 0;

            if (ixdtalen)
                bdb_state->ixdtalen[i] = ixdtalen[i];
            else
                bdb_state->ixdtalen[i] = 0;

            if (ixcollattr)
                bdb_state->ixcollattr[i] = ixcollattr[i];
            else
                bdb_state->ixcollattr[i] = 0;

            if (ixnulls)
                bdb_state->ixnulls[i] = ixnulls[i];
            else
                bdb_state->ixnulls[i] = 0;
        }

        /* determine the largest key size and the total key size */
        largest = 0;
        for (i = 0; i < numix; i++) {
            if (ixlen[i] > largest)
                largest = ixlen[i];
        }

        /* large enough to hold any key + rrn */
        bdb_state->keymaxsz = largest + sizeof(int) + (10 * sizeof(int));
    }

    /*
    if (bdb_state->attr->createdbs)
       create = 1;
    */

    bdb_state->pending_seqnum_broadcast = 0;
    Pthread_mutex_init(&bdb_state->pending_broadcast_lock, NULL);

    if (bdbtype == BDBTYPE_QUEUE || bdbtype == BDBTYPE_QUEUEDB) {
        bdb_queue_init_priv(bdb_state);
    }

    if (gbl_blob_sz_thresh_bytes != ~(0U))
        pthread_once(&bdb_blobmem_once, bdb_blobmem_init_once);
    bdb_state->bma = bdb_blobmem;
    bdb_state->bmaszthresh = gbl_blob_sz_thresh_bytes;

    if (!parent_bdb_state) {
        /* form the name of the .txn directory */
        if (bdb_state->attr->nonames) {
            sprintf(bdb_state->txndir, "%s/logs", bdb_state->dir);
            sprintf(bdb_state->tmpdir, "%s/tmp", bdb_state->dir);
        } else {
            sprintf(bdb_state->txndir, "%s/%s.txn", bdb_state->dir,
                    bdb_state->name);
            sprintf(bdb_state->tmpdir, "%s/%s.tmpdbs", bdb_state->dir,
                    bdb_state->name);
        }

        /* create transaction directory if we were told to and need to */
        if (create) {
            /* if NOT (it exists and it's a directory),
               then create the dir */
            if (!(stat(bdb_state->txndir, &sb) == 0 &&
                  (sb.st_mode & S_IFMT) == S_IFDIR)) {
                /* Create the directory */
                if (mkdir(bdb_state->txndir, 0774) != 0) {
                    print(bdb_state, "mkdir: %s: %s\n", bdb_state->txndir,
                          strerror(errno));
                    logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
                    *bdberr = BDBERR_MISC;
                    return NULL;
                }
            }
        }
        /* do this each file, even if not in create mode */
        if (!(stat(bdb_state->tmpdir, &sb) == 0 &&
              (sb.st_mode & S_IFMT) == S_IFDIR)) {
            /* Create the directory */
            if (mkdir(bdb_state->tmpdir, 0774) != 0) {
                print(bdb_state, "mkdir: %s: %s\n", bdb_state->tmpdir,
                      strerror(errno));
                logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
                *bdberr = BDBERR_MISC;
                return NULL;
            }
        }

        bdb_state->repinfo = mymalloc(sizeof(repinfo_type));
        bzero(bdb_state->repinfo, sizeof(repinfo_type));

        /* record who we are */
        bdb_state->repinfo->myhost = gbl_myhostname;

        /* we dont know who the master is yet */
        set_repinfo_master_host(bdb_state, db_eid_invalid, __func__, __LINE__);

        /* save our netinfo pointer */
        bdb_state->repinfo->netinfo = netinfo;

        /* chain back a pointer to our bdb_state for later */
        net_set_usrptr(bdb_state->repinfo->netinfo, bdb_state);

        Pthread_mutex_init(&(bdb_state->repinfo->elect_mutex), NULL);

        Pthread_mutex_init(&(bdb_state->repinfo->upgrade_lock), NULL);

        Pthread_mutex_init(&(bdb_state->repinfo->send_lock), NULL);

        Pthread_mutex_init(&(bdb_state->repinfo->receive_lock), NULL);

        Pthread_mutex_init(&(bdb_state->repinfo->appseqnum_lock), NULL);

        /* set up the appseqnum array.  we tag all packets we put out
           on the network with our own application level sequence
           number.  */
        bdb_state->repinfo->appseqnum = mymalloc((sizeof(int) * MAXNODES));
        bzero(bdb_state->repinfo->appseqnum, sizeof(int) * MAXNODES);

        for (i = 0; i < MAXNODES; i++)
            bdb_state->repinfo->appseqnum[i] = MAXNODES - i;

        bdb_set_key(bdb_state);

        /* create a blkseq db before we open the main environment,
         * since recovery routines will expect it to exist */
        if (bdb_state->attr->private_blkseq_enabled) {
            rc = bdb_create_private_blkseq(bdb_state);
            if (rc) {
                logmsg(LOGMSG_FATAL, "failed to create private blkseq rc %d\n", rc);
                exit(1);
            }
        }

        bdb_state->recoverylsn = recoverylsn;
        /*
           create a transactional environment.
           when we come back from this call, we know if we
           are the master of our replication group
        */
        bdb_state->dbenv = dbenv_open(bdb_state);
        if (bdb_state->dbenv == NULL) {
            logmsg(LOGMSG_ERROR, "dbenv_open failed\n");
            *bdberr = BDBERR_MISC;
            return NULL;
        }

        /* we are the parent/master handle */
        bdb_state->master_handle = 1;

        /* dont create all these aux helper threads for a run of initcomdb2 */
        if (!create && !gbl_exit) {
            pthread_attr_t attr;
            Pthread_attr_init(&attr);
            Pthread_attr_setstacksize(&attr, 1024 * 1024);
            /*
              create checkpoint thread.
              this thread periodically applied changes reflected in the
              log files to the database files, allowing us to remove
              log files.
              */
            rc = pthread_create(&(bdb_state->checkpoint_thread), &attr,
                                checkpoint_thread, bdb_state);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "unable to create checkpoint thread - rc=%d "
                                "errno=%d %s\n",
                        rc, errno, strerror(errno));
                logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
                *bdberr = BDBERR_MISC;
                return NULL;
            }

            /*
              create memp_trickle_thread.
              this thread tries to keep a certain amount of memory free
              so that a read can be done without incurring a last minute
              write in an effort to make memory available for the read
              */
            rc = pthread_create(&(bdb_state->memp_trickle_thread), &attr,
                                memp_trickle_thread, bdb_state);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "unable to create memp_trickle thread - rc=%d "
                                "errno=%d %s\n",
                        rc, errno, strerror(errno));
                logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
                *bdberr = BDBERR_MISC;
                return NULL;
            }

            /* create the deadlock detect thread if we arent doing auto
               deadlock detection */
            if (!bdb_state->attr->autodeadlockdetect) {
                rc = pthread_create(&dummy_tid, &attr, deadlockdetect_thread,
                                    bdb_state);
            }

            if (bdb_state->attr->coherency_lease) {
                create_coherency_lease_thread(bdb_state);
            }

            if (bdb_state->attr->master_lease) {
                create_master_lease_thread(bdb_state);
            }

            /*
              create log deletion thread.
              this thread periodically checks for logs older than the
              specified age, and deletes them.
              */
            if (!is_real_netinfo(bdb_state->repinfo->netinfo))
            /*NOTE: don't DELETE LOGS while running RECOVERY */
            {

                if (!gbl_fullrecovery) {
                    print(bdb_state, "will not keep logfiles\n");
                    rc = bdb_state->dbenv->set_flags(bdb_state->dbenv,
                                                     DB_LOG_AUTOREMOVE, 1);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR, "set_flags failed\n");
                        *bdberr = BDBERR_MISC;
                        return NULL;
                    }
                } else {
                    print(bdb_state,
                          "running recovery, not deleting log files\n");
                }
            } else {
                print(bdb_state,
                      "logfiles will be deleted in logdelete_thread\n");
                rc = pthread_create(&(bdb_state->logdelete_thread), &attr,
                                    logdelete_thread, bdb_state);
                if (rc) {
                    logmsg(LOGMSG_ERROR,
                           "unable to create logdelete thread rc %d %s\n", rc,
                           strerror(rc));
                    *bdberr = BDBERR_MISC;
                    return NULL;
                }
            }

            Pthread_attr_destroy(&attr);
        }

        /* This bit needs to be exclusive.  We don't want replication messages
         * flipping us in/out of being master at this point, or we just end up
         * in a confused state (and parent bdb_state ended
         * up read_write==0, all child bdb_states had read_write=1).
         */
        BDB_WRITELOCK("bdb_open_int");

        char *master;
        uint32_t gen, egen;

        if (bdb_get_rep_master(bdb_state, &master, &gen, &egen) == 0 &&
            net_get_mynode(bdb_state->repinfo->netinfo) == master) {
            logmsg(LOGMSG_INFO, "%s:%d read_write = 1\n", __FILE__, __LINE__);
            iammaster = 1;
        } else
            iammaster = 0;

        if (is_real_netinfo(bdb_state->repinfo->netinfo) && iammaster) {

            logmsg(LOGMSG_USER,
                   "%s line %d calling rep_start as master with egen %d\n",
                   __func__, __LINE__, gen);
            rc = bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, gen,
                                             DB_REP_MASTER);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "rep_start as master failed %d %s\n", rc,
                        db_strerror(rc));
            } else {
                print(bdb_state, "bdb_open_int: started rep as MASTER\n");
            }
            defer_commits_for_upgrade(bdb_state, 0, __func__);
        }

        /* we used to blindly set read_write to 1 on startup.  this caused
         * mayhem when we tried stopping unnecessary upgrades -- SJ */
        bdb_state->read_write = iammaster ? 1 : 0;
        bdb_state->envonly = 1;

        bdb_state->repinfo->upgrade_allowed = 1;

        whoismaster_rtn(bdb_state, 1);

        logmsg(
            LOGMSG_INFO, "@LSN %u:%u\n",
            bdb_state->seqnum_info->seqnums[nodeix(gbl_myhostname)].lsn.file,
            bdb_state->seqnum_info->seqnums[nodeix(gbl_myhostname)].lsn.offset);

        BDB_RELLOCK();
    } else {
        /* make sure our parent came from a real bdb_open() call. */
        if (!parent_bdb_state->master_handle) {
            logmsg(LOGMSG_FATAL, "open more with child passed as parent!\n");
            exit(1);
        }

        /* remember who our parent is, so we can find him later */
        bdb_state->parent = parent_bdb_state;

        /* share the dbenv with our parent */
        bdb_state->dbenv = parent_bdb_state->dbenv;

        /* share the repinfo with our parent */
        bdb_state->repinfo = parent_bdb_state->repinfo;

        /* initialize this thing high so any findnexts that happen before we
           get a broadcast from master will not skip anything */
        bdb_state->master_cmpcontext = flibc_htonll(ULLONG_MAX);

        /* do not inherit compress or compress_blobs from parent -
           values were initialised above */

        /* Determine our masterfulness. */

        if (net_get_mynode(bdb_state->repinfo->netinfo) ==
            bdb_state->repinfo->master_host)
            iammaster = 1;
        else
            iammaster = 0;

        /* open our databases as either a client or master */
        bdb_state->bdbtype = bdbtype;
        bdb_state->pagesize_override = pagesize_override;
        rc = open_dbs(bdb_state, iammaster, upgrade, create, tid, flags);
        if (rc != 0) {
            if (bdb_state->parent) {
                free(bdb_state);
                logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
                *bdberr = BDBERR_MISC;
                return NULL;
            } else {
                logmsg(LOGMSG_FATAL, "error opening parent\n");
                exit(1);
            }
        }

        if (!temp && bdb_state->parent) {
            int chained = 0;
            bdb_state_type *parent;

            parent = bdb_state->parent;

            bdb_lock_children_lock(parent);

            /* chain us into a free slot, or extend */
            for (i = 0; i < parent->numchildren; i++) {
                if (parent->children[i] == NULL) {
                    parent->children[i] = bdb_state;
                    chained = 1;
                    ctrace("bdb_open_int took free slot %d\n", i);
                    break;
                }
            }
            if (!chained) {
                parent->children[bdb_state->parent->numchildren] = bdb_state;
                ctrace("bdb_open_int took last slot %d and extended\n",
                       parent->numchildren);
                parent->numchildren++;
                assert(parent->numchildren < MAX_CHILDREN);
            }

            bdb_unlock_children_lock(parent);
        }

        bdb_state->last_dta = 0;
        Pthread_mutex_init(&bdb_state->last_dta_lk, NULL);
    }

    bdb_state->isopen = 1;

    if (bdbtype == BDBTYPE_QUEUEDB) {
        if (!bdb_trigger_ispaused(bdb_state)) {
            bdb_trigger_open(bdb_state);
        }
    }

    if (bdb_state->attr->dtastripe && (!bdb_state->attr->genids)) {
        logmsg(LOGMSG_WARN, "dtastripe implies genids!\n");
    }

    if (bdb_state->parent == NULL && !bdb_state->attr->dont_report_deadlock)
        berkdb_register_deadlock_callback(deadlock_happened);

    return bdb_state;
}

static pthread_once_t once_init_master_strings = PTHREAD_ONCE_INIT;
char *bdb_master_dupe;
static void init_eid_strings(void)
{
    bdb_master_dupe = intern(".master_dupe");
    db_eid_broadcast = intern(".broadcast");
    db_eid_invalid = intern(".invalid");
}

bdb_state_type *bdb_open_env(const char name[], const char dir[],
                             bdb_attr_type *bdb_attr,
                             bdb_callback_type *bdb_callback, void *usr_ptr,
                             netinfo_type *netinfo, char *recoverlsn,
                             int *bdberr)
{
    *bdberr = BDBERR_NOERROR;

    if (netinfo == NULL) {
        netinfo = create_netinfo_fake();
    }

    pthread_once(&once_init_master_strings, init_eid_strings);

    if (bdb_attr == NULL)
        bdb_attr = bdb_attr_create();

    if (bdb_callback == NULL)
        bdb_callback = bdb_callback_create();

    return bdb_open_int(
        1, /* envonly */
        name, dir, 0, 0, NULL, NULL, NULL, NULL, NULL,
        NULL,         /* numix, ixlen, ixdups, ixrecnum, ixdta, ixdtalen, ixcollattr */
        NULL,         /* ixnulls */
        0,            /* numdtafiles */
        bdb_attr,     /* bdb_attr */
        bdb_callback, /* bdb_callback */
        usr_ptr,      /* usr_ptr */
        netinfo,      /* netinfo */
        0,            /* upgrade */
        bdb_attr->createdbs, /* create */
        bdberr, NULL,        /* parent_bdb_handle */
        0, BDBTYPE_ENV, NULL, 0, recoverlsn, 0);
}

bdb_state_type *
bdb_create_tran(const char name[], const char dir[], int lrl, short numix,
                const short ixlen[], const signed char ixdups[],
                const signed char ixrecnum[], const signed char ixdta[], const int ixdtalen[],
                const signed char ixcollattr[], const signed char ixnulls[],
                int numdtafiles, bdb_state_type *parent_bdb_handle, int temp,
                int *bdberr, tran_type *trans)
{
    DB_TXN *tid = trans ? trans->tid : NULL;
    bdb_state_type *bdb_state, *ret;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_handle;

    if (!temp) {
        BDB_READLOCK("bdb_create");

        ret =
            bdb_open_int(0, /* envonly */
                         name, dir, lrl, numix, ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                         ixcollattr, ixnulls, numdtafiles, NULL, /* bdb_attr */
                         NULL, /* bdb_callback */
                         NULL, /* usr_ptr */
                         NULL, /* netinfo */
                         0,    /* upgrade */
                         1,    /* create */
                         bdberr, parent_bdb_handle, 0, BDBTYPE_TABLE, tid, 0,
                         NULL, /* open lite options */
                         0);

        BDB_RELLOCK();
    } else {
        ret =
            bdb_open_int(0, /* envonly */
                         name, dir, lrl, numix, ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                         ixcollattr, ixnulls, numdtafiles, NULL, /* bdb_attr */
                         NULL, /* bdb_callback */
                         NULL, /* usr_ptr */
                         NULL, /* netinfo */
                         0,    /* upgrade */
                         1,    /* create */
                         bdberr, parent_bdb_handle, 0, BDBTYPE_TABLE, NULL, 1,
                         NULL, /* open lite options */
                         0);
    }

    return ret;
}

/* open another database in the same transaction/replication
   environment as the parent bdb_state */
bdb_state_type *
bdb_open_more_int(const char name[], const char dir[], int lrl, short numix,
                  const short ixlen[], const signed char ixdups[],
                  const signed char ixrecnum[], const signed char ixdta[], const int ixdtalen[],
                  const signed char ixcollattr[], const signed char ixnulls[],
                  int numdtafiles, bdb_state_type *parent_bdb_handle,
                  int *bdberr)
{
    bdb_state_type *ret;

    *bdberr = BDBERR_NOERROR;

    ret = bdb_open_int(0, /* envonly */
                       name, dir, lrl, numix, ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                       ixcollattr, ixnulls, numdtafiles, NULL, /* bdb_attr */
                       NULL,                               /* bdb_callback */
                       NULL,                               /* usr_ptr */
                       NULL,                               /* netinfo */
                       0,                                  /* upgrade */
                       parent_bdb_handle->attr->createdbs, /* create */
                       bdberr, parent_bdb_handle, 0, /* pagesize override */
                       BDBTYPE_TABLE, NULL, 0, NULL, 0);

    return ret;
}

bdb_state_type *
bdb_create(const char name[], const char dir[], int lrl, short numix,
           const short ixlen[], const signed char ixdups[],
           const signed char ixrecnum[], const signed char ixdta[], const int ixdtalen[],
           const signed char ixcollattr[], const signed char ixnulls[],
           int numdtafiles, bdb_state_type *parent_bdb_handle, int temp,
           int *bdberr)
{
    return bdb_create_tran(name, dir, lrl, numix, ixlen, ixdups, ixrecnum,
                           ixdta, ixdtalen, ixcollattr, ixnulls, numdtafiles,
                           parent_bdb_handle, temp, bdberr, NULL);
}

/* open another database in the same transaction/replication
   environment as the parent bdb_state */
bdb_state_type *
bdb_open_more(const char name[], const char dir[], int lrl, short numix,
              const short ixlen[], const signed char ixdups[],
              const signed char ixrecnum[], const signed char ixdta[], const int ixdtalen[],
              const signed char ixcollattr[], const signed char ixnulls[],
              int numdtafiles, bdb_state_type *parent_bdb_handle, int *bdberr)
{
    bdb_state_type *bdb_state, *ret;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_handle;
    BDB_READLOCK("bdb_open_more");

    ret = bdb_open_int(0, /* envonly */
                       name, dir, lrl, numix, ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                       ixcollattr, ixnulls, numdtafiles, NULL, /* bdb_attr */
                       NULL,                               /* bdb_callback */
                       NULL,                               /* usr_ptr */
                       NULL,                               /* netinfo */
                       0,                                  /* upgrade */
                       parent_bdb_handle->attr->createdbs, /* create */
                       bdberr, parent_bdb_handle, 0, /* pagesize override */
                       BDBTYPE_TABLE, NULL, 0, NULL, 0);

    BDB_RELLOCK();

    return ret;
}

/* open another database in the same transaction/replication
   environment as the parent bdb_state */
bdb_state_type *
bdb_open_more_tran(const char name[], const char dir[], int lrl, short numix,
                   const short ixlen[], const signed char ixdups[],
                   const signed char ixrecnum[], const signed char ixdta[], const int ixdtalen[],
                   const signed char ixcollattr[], const signed char ixnulls[],
                   int numdtafiles, bdb_state_type *parent_bdb_handle,
                   tran_type *tran, uint32_t flags, int *bdberr)
{
    bdb_state_type *bdb_state, *ret;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_handle;
    BDB_READLOCK("bdb_open_more_tran");

    ret = bdb_open_int(0, /* envonly */
                       name, dir, lrl, numix, ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                       ixcollattr, ixnulls, numdtafiles, NULL, /* bdb_attr */
                       NULL,                               /* bdb_callback */
                       NULL,                               /* usr_ptr */
                       NULL,                               /* netinfo */
                       0,                                  /* upgrade */
                       parent_bdb_handle->attr->createdbs, /* create */

                       bdberr, parent_bdb_handle, 0, /* pagesize override */
                       BDBTYPE_TABLE, tran ? tran->tid : NULL, 0, NULL, flags);

    BDB_RELLOCK();

    return ret;
}

int get_seqnum(bdb_state_type *bdb_state, const char *host)
{
    int seq;

    Pthread_mutex_lock(&(bdb_state->repinfo->appseqnum_lock));

    bdb_state->repinfo->appseqnum[nodeix(host)]++;
    seq = bdb_state->repinfo->appseqnum[nodeix(host)];

    Pthread_mutex_unlock(&(bdb_state->repinfo->appseqnum_lock));

    return seq;
}

bdb_state_type *bdb_open_more_lite(const char name[], const char dir[], int lrl,
                                   int ixlen_in, int pagesize,
                                   bdb_state_type *parent_bdb_handle,
                                   tran_type *tran, uint32_t flags, int *bdberr)
{
    int numdtafiles = 1;
    short numix = 1;
    signed char ixdups[1] = {0};
    signed char ixrecnum[1] = {0};
    signed char ixdta[1] = {0};
    int ixdtalen[1] = {0};
    signed char ixnulls[1] = {0};
    short ixlen;

    ixlen = ixlen_in;

    bdb_state_type *bdb_state, *ret;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_handle;
    BDB_READLOCK("bdb_open_more_lite");

    ret = bdb_open_int(0, /* envonly */
                       name, dir, lrl, numix, &ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                       NULL, ixnulls, numdtafiles, NULL,   /* bdb_attr */
                       NULL,                               /* bdb_callback */
                       NULL,                               /* usr_ptr */
                       NULL,                               /* netinfo */
                       0,                                  /* upgrade */
                       parent_bdb_handle->attr->createdbs, /* create */
                       bdberr, parent_bdb_handle, pagesize, BDBTYPE_LITE,
                       tran ? tran->tid : NULL, 0, NULL, flags);

    BDB_RELLOCK();

    return ret;
}

bdb_state_type *bdb_open_more_queue(const char name[], const char dir[],
                                    int item_size, int pagesize,
                                    bdb_state_type *parent_bdb_state,
                                    int isqueuedb, tran_type *tran, int *bdberr)
{
    bdb_state_type *bdb_state, *ret = NULL;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_state;
    BDB_READLOCK("bdb_open_more_queue");

    ret =
        bdb_open_int(0,                    /* env only */
                     name, dir, item_size, /* pass item_size in as lrl */
                     0,                    /* numix */
                     NULL,                 /* ixlen */
                     NULL,                 /* ixdups */
                     NULL,                 /* ixrecnum */
                     NULL,                 /* ixdta */
                     NULL,                 /* ixdtalen */
                     NULL,                 /* ixcollattr */
                     NULL,                 /* ixnulls */
                     1,                    /* numdtafiles (berkdb queue file) */
                     NULL,                 /* bdb_attr */
                     NULL,                 /* bdb_callback */
                     NULL,                 /* usr_ptr */
                     NULL,                 /* netinfo */
                     0,                    /* upgrade */
                     parent_bdb_state->attr->createdbs,  /* create */
                     bdberr, parent_bdb_state, pagesize, /* pagesize override */
                     isqueuedb ? BDBTYPE_QUEUEDB : BDBTYPE_QUEUE,
                     tran ? tran->tid : NULL, 0, NULL, 0);

    BDB_RELLOCK();

    return ret;
}

bdb_state_type *bdb_create_queue_tran(tran_type *tran, const char name[],
                                      const char dir[], int item_size,
                                      int pagesize,
                                      bdb_state_type *parent_bdb_state,
                                      int isqueuedb, int *bdberr)
{
    DB_TXN *tid = tran ? tran->tid : NULL;
    bdb_state_type *bdb_state, *ret = NULL;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_state;
    BDB_READLOCK("bdb_create_queue");

    ret = bdb_open_int(
        0,                    /* env only */
        name, dir, item_size, /* pass item_size in as lrl */
        0,                    /* numix */
        NULL,                 /* ixlen */
        NULL,                 /* ixdups */
        NULL,                 /* ixrecnum */
        NULL,                 /* ixdta */
        NULL,                 /* ixdtalen */
        NULL,                 /* ixcollattr */
        NULL,                 /* ixnulls */
        1,                    /* numdtafiles (berkdb queue file) */
        NULL,                 /* bdb_attr */
        NULL,                 /* bdb_callback */
        NULL,                 /* usr_ptr */
        NULL,                 /* netinfo */
        0,                    /* upgrade */
        1,                    /* create */
        bdberr, parent_bdb_state, pagesize, /* pagesize override */
        isqueuedb ? BDBTYPE_QUEUEDB : BDBTYPE_QUEUE, tid, 0, NULL, 0);

    BDB_RELLOCK();

    return ret;
}

bdb_state_type *bdb_create_queue(const char name[], const char dir[],
                                 int item_size, int pagesize,
                                 bdb_state_type *parent_bdb_state,
                                 int isqueuedb, int *bdberr)
{
    return bdb_create_queue_tran(NULL, name, dir, item_size, pagesize,
                                 parent_bdb_state, isqueuedb, bdberr);
}

bdb_state_type *bdb_create_more_lite(const char name[], const char dir[],
                                     int lrl, int ixlen_in, int pagesize,
                                     bdb_state_type *parent_bdb_handle,
                                     int *bdberr)
{
    int numdtafiles = 1;
    short numix = 1;
    signed char ixdups[1] = {0};
    signed char ixrecnum[1] = {0};
    signed char ixdta[1] = {0};
    int ixdtalen[1] = {0};
    signed char ixnulls[1] = {0};
    short ixlen;

    ixlen = ixlen_in;

    bdb_state_type *bdb_state, *ret;

    *bdberr = BDBERR_NOERROR;

    bdb_state = parent_bdb_handle;
    BDB_READLOCK("bdb_create_more_lite");

    /* Only master can do this */
    if (!parent_bdb_handle->read_write) {
        *bdberr = BDBERR_READONLY;
        ret = 0;
    } else {
        ret =
            bdb_open_int(0, /* envonly */
                         name, dir, lrl, numix, &ixlen, ixdups, ixrecnum, ixdta, ixdtalen,
                         NULL, ixnulls, numdtafiles, NULL, /* bdb_attr */
                         NULL,                             /* bdb_callback */
                         NULL,                             /* usr_ptr */
                         NULL,                             /* netinfo */
                         0,                                /* upgrade */
                         1,                                /* create */
                         bdberr, parent_bdb_handle, pagesize, BDBTYPE_LITE,
                         NULL, 0, NULL, 0);
    }

    BDB_RELLOCK();

    return ret;
}

void bdb_remove_fileid_pglogs(bdb_state_type *bdb_state, unsigned char *fileid);

/* Pass in mangled file name, this will delete it. */
static int bdb_del_file(bdb_state_type *bdb_state, DB_TXN *tid, char *filename,
                        int *bdberr)
{
    DB_ENV *dbenv;
    DB *dbp;
    char transname[PATH_MAX];
    char *pname = bdb_trans(filename, transname);
    int rc = 0;

    if (bdb_state->parent)
        dbenv = bdb_state->parent->dbenv;
    else
        dbenv = bdb_state->dbenv;

    if ((rc = access(pname, F_OK)) == 0) {
        int rc;

        if ((rc = db_create(&dbp, dbenv, 0)) == 0 &&
            (rc = dbp->open(dbp, NULL, pname, NULL, DB_BTREE, 0, 0666)) == 0) {
            bdb_remove_fileid_pglogs(bdb_state, dbp->fileid);
            dbp->close(dbp, DB_NOSYNC);
        }

        rc = dbenv->dbremove(dbenv, tid, filename, NULL, 0);
        if (rc) {
           logmsg(LOGMSG_ERROR, "bdb_del_file: dbremove %s failed: %d %s\n", filename, rc,
                   db_strerror(rc));
            if (rc == ENOENT)
                *bdberr = BDBERR_DELNOTFOUND;
            else {
                logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
                *bdberr = BDBERR_MISC;
            }
            rc = -1;
        } else {
            print(bdb_state, "bdb_del_file: removed %s\n", filename);
        }

    } else {
        logmsg(LOGMSG_ERROR, "bdb_del_file: cannot access %s: %d %s\n", pname, errno,
                strerror(errno));
        if (errno == ENOENT)
            *bdberr = BDBERR_DELNOTFOUND;
        else {
            logmsg(LOGMSG_INFO, "%s failing with bdberr_misc at line %d\n", __func__, __LINE__);
            *bdberr = BDBERR_MISC;
        }

        rc = -1;
    }
    return rc;
}

static int bdb_del_data_int(bdb_state_type *bdb_state, DB_TXN *tid, int dtanum,
                            int *bdberr)
{
    int strnum;
    char newname[80];
    *bdberr = BDBERR_NOERROR;
    for (strnum = bdb_get_datafile_num_files(bdb_state, dtanum) - 1;
         strnum >= 0; strnum--) {
        form_datafile_name(bdb_state, tid, dtanum, strnum, newname,
                           sizeof(newname));
        if (bdb_del_file(bdb_state, tid, newname, bdberr) != 0)
            return -1;
    }
    return 0;
}

static int bdb_del_ix_int(bdb_state_type *bdb_state, DB_TXN *tid, int ixnum,
                          int *bdberr)
{
    char newname[80];
    *bdberr = BDBERR_NOERROR;

    form_indexfile_name(bdb_state, tid, ixnum, newname, sizeof(newname));

    if (bdb_del_file(bdb_state, tid, newname, bdberr) != 0)
        return -1;
    return 0;
}

static int bdb_del_int(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    int rc = 0;
    int i;
    int dtanum;
    DB_TXN *tid = tran->tid;

    *bdberr = BDBERR_NOERROR;

    logmsg(LOGMSG_DEBUG, "bdb_del %s\n", bdb_state->name);

    if (bdb_state->bdbtype == BDBTYPE_TABLE ||
        bdb_state->bdbtype == BDBTYPE_LITE) {
        /* remove data files */
        for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++)
            if (0 != bdb_del_data_int(bdb_state, tid, dtanum, bdberr))
                return -1;

        /* remove the index files */
        for (i = 0; i < bdb_state->numix; i++)
            if (0 != bdb_del_ix_int(bdb_state, tid, i, bdberr))
                return -1;
    } else if (bdb_state->bdbtype == BDBTYPE_QUEUEDB) {
        assert(BDB_QUEUEDB_MAX_FILES == 2); // TODO: Hard-coded for now.
        for (int dtanum = 0; dtanum < BDB_QUEUEDB_MAX_FILES; dtanum++) {
            unsigned long long old_qdb_file_ver;
            if (should_stop_looking_for_queuedb_files(bdb_state, tran,
                                                      dtanum,
                                                      &old_qdb_file_ver)) {
                break;
            }
            char name[PATH_MAX];
            form_queuedb_name_int(
                bdb_state, name, sizeof(name), old_qdb_file_ver
            );
            rc = bdb_del_file(bdb_state, tid, name, bdberr);
            if (rc != 0) break;
        }
    }

    return rc;
}

int bdb_del(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_del");
    rc = bdb_del_int(bdb_state, tran, bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_del_data(bdb_state_type *bdb_state, tran_type *tran, int dtanum,
                 int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_del_data");
    rc = bdb_del_data_int(bdb_state, tran->tid, dtanum, bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_del_ix(bdb_state_type *bdb_state, tran_type *tran, int ixnum,
               int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_del_ix");
    rc = bdb_del_ix_int(bdb_state, tran->tid, ixnum, bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_rename_file(bdb_state_type *bdb_state, DB_TXN *tid, char *oldfile,
                    char *newfile, int *bdberr)
{
    DB_ENV *dbenv;
    int rc;
    if (bdb_state->parent)
        dbenv = bdb_state->parent->dbenv;
    else
        dbenv = bdb_state->dbenv;
    rc = dbenv->dbrename(bdb_state->dbenv, tid, oldfile, NULL, newfile, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_rename_file: dbrename failed %s->%s %d %s\n",
                oldfile, newfile, rc, db_strerror(rc));
        *bdberr = BDBERR_MISC;
        return -1;
    }
    print(bdb_state, "bdb_rename_file: dbrenamed %s->%s\n", oldfile, newfile);
    return 0;
}

int bdb_rename_data_int(bdb_state_type *bdb_state, tran_type *tran,
                        char newtablename[], int fromdtanum, int todtanum,
                        int *bdberr)
{
    int strnum;
    char newname[80];
    char oldname[80];
    char *orig_name = bdb_state->name;

    *bdberr = BDBERR_NOERROR;

    /* Never turn blobs into non-blobs or vice versa, that would be silly */
    if ((fromdtanum == 0 && todtanum != 0) ||
        (fromdtanum != 0 && todtanum == 0)) {
        logmsg(LOGMSG_ERROR, "bdb_rename_data_int: illegal from/to %d->%d\n",
                fromdtanum, todtanum);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    for (strnum = bdb_get_datafile_num_files(bdb_state, fromdtanum) - 1;
         strnum >= 0; strnum--) {
        form_datafile_name(bdb_state, tran->tid, fromdtanum, strnum, oldname,
                           sizeof(oldname));
        bdb_state->name = newtablename; /* temporarily change tbl name */
        form_datafile_name(bdb_state, tran->tid, todtanum, strnum, newname,
                           sizeof(newname));
        bdb_state->name = orig_name; /* revert name to original */
        if (0 !=
            bdb_rename_file(bdb_state, tran->tid, oldname, newname, bdberr))
            return -1;
    }
    return 0;
}

int bdb_rename_ix_int(bdb_state_type *bdb_state, tran_type *tran,
                      char newtablename[], int fromixnum, int toixnum,
                      int *bdberr)
{
    char newname[80];
    char oldname[80];
    char *orig_name = bdb_state->name;

    *bdberr = BDBERR_NOERROR;

    form_indexfile_name(bdb_state, tran->tid, fromixnum, oldname,
                        sizeof(oldname));
    bdb_state->name = newtablename; /* temporarily change tbl name */
    form_indexfile_name(bdb_state, tran->tid, toixnum, newname,
                        sizeof(newname));
    bdb_state->name = orig_name; /* revert name to original */

    if (0 != bdb_rename_file(bdb_state, tran->tid, oldname, newname, bdberr))
        return -1;

    return 0;
}

/*
  XXX.<bdb_state:tablename>.dta                -> XXX.<newtablename>.dta
  XXX.<bdb_state:tablename>.dta1               -> XXX.<newtablename>.dta1
  XXX.<bdb_state:tablename>.ix0                -> XXX.<newtablename>.ix0
  XXX.<bdb_state:tablename>.ix1                -> XXX.<newtablename>.ix1
*/
int bdb_rename_int(bdb_state_type *bdb_state, tran_type *tran,
                   char newtablename[], int *bdberr)
{
    int i;
    int dtanum;

    logmsg(LOGMSG_DEBUG, "bdb_rename %s -> %s\n", bdb_state->name, newtablename);
    *bdberr = BDBERR_NOERROR;

    /* rename data files */
    for (dtanum = 0; dtanum < bdb_state->numdtafiles; dtanum++)
        if (0 != bdb_rename_data_int(bdb_state, tran, newtablename, dtanum,
                                     dtanum, bdberr))
            return -1;

    /* rename index files */
    for (i = 0; i < bdb_state->numix; i++)
        if (0 != bdb_rename_ix_int(bdb_state, tran, newtablename, i, i, bdberr))
            return -1;

    free(bdb_state->name);
    bdb_state->name = strdup(newtablename);

    return 0;
}

int bdb_rename(bdb_state_type *bdb_state, tran_type *tran, char newtablename[],
               int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_rename");
    rc = bdb_rename_int(bdb_state, tran, newtablename, bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_rename_data(bdb_state_type *bdb_state, tran_type *tran,
                    char newtablename[], int fromdtanum, int todtanum,
                    int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_rename_data");
    rc = bdb_rename_data_int(bdb_state, tran, newtablename, fromdtanum,
                             todtanum, bdberr);
    BDB_RELLOCK();
    return rc;
}

int bdb_rename_ix(bdb_state_type *bdb_state, tran_type *tran,
                  char newtablename[], int fromixnum, int toixnum, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_rename_ix");
    rc = bdb_rename_ix_int(bdb_state, tran, newtablename, fromixnum, toixnum,
                           bdberr);
    BDB_RELLOCK();
    return rc;
}

void bdb_state_rename(bdb_state_type *bdb_state, char *newname)
{
    char *old = bdb_state->name;
    bdb_state->name = newname;
    free(old);
}

static int bdb_rename_blob1_int(bdb_state_type *bdb_state, tran_type *tran,
                                unsigned long long *genid, int *bdberr)
{
    int dtanum;
    for (dtanum = 1; dtanum < bdb_state->numdtafiles; dtanum++) {
        char sfx[] = "s0";
        char oldname[100];
        char newname[sizeof(oldname) + sizeof(sfx)];

        /* form old (current) name */
        form_datafile_name(bdb_state, tran->tid, dtanum, 0 /*stripenum*/,
                           oldname, sizeof(oldname));

/* form new name */
#if 0
      size_t namelen = strlen( oldname );
      strncpy0( newname, oldname, sizeof( newname ) );

      /* if the name is not empty and it doesn't end with 0 and there is enough
       * room to add another character */
      if( namelen > 0 && newname[ namelen - 1 ] != '0' &&
              sizeof( newname ) > namelen + 1 )
      {
          /*add a 0 to the end of the string*/
          newname[ namelen + 1 ] = '\0';
          newname[ namelen ] = '0';
      }
#else
        snprintf(newname, sizeof newname, "%s%s", oldname, sfx);
#endif

        if (0 !=
            bdb_rename_file(bdb_state, tran->tid, oldname, newname, bdberr))
            return -1;
    }

    /* record the time of this conversion */
    bdb_state->blobstripe_convert_genid = get_genid(bdb_state, 0);
    *genid = bdb_state->blobstripe_convert_genid;

    return 0;
}

int bdb_rename_blob1(bdb_state_type *bdb_state, tran_type *tran,
                     unsigned long long *genid, int *bdberr)
{
    int rc = 0;
    BDB_READLOCK("bdb_rename_blob1");
    rc = bdb_rename_blob1_int(bdb_state, tran, genid, bdberr);
    BDB_RELLOCK();
    return rc;
}

void bdb_set_blobstripe_genid(bdb_state_type *bdb_state,
                              unsigned long long genid)
{
    bdb_state->blobstripe_convert_genid = genid;
}

int bdb_close_temp_state(bdb_state_type *bdb_state, int *bdberr)
{
    int rc;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->envonly)
        return 0;

    /* close doesn't fail */
    rc = close_dbs(bdb_state);

    return rc;
}

void bdb_replace_handle(bdb_state_type *parent, int ix, bdb_state_type *handle)
{
    parent->children[ix] = handle;
}

int get_dbnum_by_handle(bdb_state_type *bdb_state)
{
    int i;

    bdb_lock_children_lock(bdb_state);

    for (i = 0; i < bdb_state->parent->numchildren; i++)
        if (bdb_state->parent->children[i] == bdb_state) {
            bdb_unlock_children_lock(bdb_state);
            return i;
        }

    bdb_unlock_children_lock(bdb_state);

    return -1;
}

int get_dbnum_by_name(bdb_state_type *bdb_state, const char *name)
{
    int i;
    int nlen = strlen(name);
    int found = -1;

    bdb_lock_children_lock(bdb_state);

    for (i = 0; i < bdb_state->parent->numchildren; i++) {
        if (strncasecmp(bdb_state->parent->children[i]->name, name, nlen) ==
            0) {
            found = i;
            break;
        }
    }

    bdb_unlock_children_lock(bdb_state);
    return found;
}

static int bdb_close_only_flags(bdb_state_type *bdb_state, DB_TXN *tid, int *bdberr, int flags)
{
    int i;
    bdb_state_type *parent;

    *bdberr = BDBERR_NOERROR;

    logmsg(LOGMSG_DEBUG, "bdb_close_only_int called on %s\n", bdb_state->name);

    /* lets only free children/tables, not the parent/environment for now */
    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        return 0;

    /* close doesn't fail */
    if (flags & BDB_CLOSE_FLAGS_FLUSH) {
       close_dbs_flush(bdb_state);
    } else {
       close_dbs(bdb_state);
    }

    /* now remove myself from my parents list of children */

    bdb_lock_children_lock(parent);

    /* find ourselves and swap null it. */
    for (i = 0; i < parent->numchildren; i++)
        if (parent->children[i] == bdb_state) {
            logmsg(LOGMSG_DEBUG, "bdb_close_only_int freeing slot %d\n", i);
            parent->children[i] = NULL;
            break;
        }

    bdb_unlock_children_lock(parent);

    return 0;
}

static int bdb_close_only_int(bdb_state_type *bdb_state, DB_TXN *tid, int *bdberr)
{
    return bdb_close_only_flags(bdb_state, tid, bdberr, 0);
}

int bdb_close_only_sc(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    int rc;

    if (bdb_state->envonly)
        return 0;

    BDB_READLOCK("bdb_close_only_sc");

    rc = bdb_close_only_int(bdb_state, tran ? tran->tid : NULL, bdberr);

    BDB_RELLOCK();

    return rc;
}

int bdb_close_only(bdb_state_type *bdb_state, int *bdberr)
{
    int rc;

    if (!bdb_state || bdb_state->envonly)
        return 0;

    BDB_READLOCK("bdb_close_only");

    rc = bdb_close_only_int(bdb_state, NULL, bdberr);

    BDB_RELLOCK();

    return rc;
}

int bdb_close_flush(bdb_state_type *bdb_state, int *bdberr)
{
    if (!bdb_state || bdb_state->envonly) return 0;
    BDB_READLOCK(__func__);
    int rc = bdb_close_only_flags(bdb_state, NULL, bdberr, BDB_CLOSE_FLAGS_FLUSH);
    BDB_RELLOCK();
    return rc;
}

/* bdb_state is freed after this call */
static int bdb_free_int(bdb_state_type *bdb_state, bdb_state_type *replace,
                        int *bdberr)
{
    bdb_state_type *child = NULL;

    /* dont free open bdb handles */
    if (bdb_state->isopen) {
        print(bdb_state, "bdb_free_int(%s) isopen, not freeing\n",
              bdb_state->name);
        return -1;
    }

    bdb_state->exiting = 1;

    if (bdb_state->parent)
        child = bdb_state;

    /* if its a child handle, free it */
    if (child) {
        bdb_state = bdb_state->parent;
        free(child->origname);
        free(child->name);
        free(child->dir);
        free(child->txndir);
        free(child->tmpdir);
        free(child->fld_hints);
        for (int i = 0; i < child->numix; ++i) {
            free(child->fld_hints_pd[i]);
        }

        // free bthash
        bdb_handle_dbp_drop_hash(child);
        memset(child, 0xff, sizeof(bdb_state_type));

        if (replace) {
            memcpy(child, replace, sizeof(bdb_state_type));

            bdb_lock_children_lock(bdb_state);

            /* find ourselves and swap it. */
            for (int i = 0; i < bdb_state->numchildren; i++)
                if (bdb_state->children[i] == replace) {
                    logmsg(LOGMSG_DEBUG, "%d swapping %p with %p\n", i, replace,
                           child);
                    bdb_state->children[i] = child;
                    break;
                }

            bdb_unlock_children_lock(bdb_state);
        } else
            free(child);
    }

    return 0;
}

int bdb_free(bdb_state_type *bdb_state, int *bdberr)
{
    int rc;
    bdb_state_type *parent;

    parent = bdb_state->parent;

    BDB_READLOCK("bdb_free");

    rc = bdb_free_int(bdb_state, NULL, bdberr);

    if (parent) {
        /* If we just freed a child table then we should make sure that the
         * BDB_RELLOCK() macro doesn't try to dereference pointers in the
         * memory that we just freed. */
        bdb_state = parent;
        BDB_RELLOCK();
    } else {
        /* If we just freed the environment then we can't release the write lock
         * at all since it lives in freed memory.  I don't think comdb2 ever
         * hits this code path anyway. */
    }

    return rc;
}

int bdb_free_and_replace(bdb_state_type *bdb_state, bdb_state_type *replace,
                         int *bdberr)
{
    int rc;
    bdb_state_type *parent;

    parent = bdb_state->parent;

    BDB_READLOCK("bdb_free_and_replace");

    rc = bdb_free_int(bdb_state, replace, bdberr);

    if (parent) {
        bdb_state = parent;
        BDB_RELLOCK();
    }

    return rc;
}

/* re-open bdb handle as master/client depending on how it used to be */
int bdb_open_again_tran_int(bdb_state_type *bdb_state, DB_TXN *tid,
                            uint32_t flags, int *bdberr)
{
    int iammaster;
    int rc;
    int chained = 0;
    int i;
    bdb_state_type *parent;

    BDB_READLOCK("bdb_open_again");

    if (!bdb_state->parent) {
        logmsg(LOGMSG_ERROR, "bdb_open_again_tran_int called on parent\n");
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    parent = bdb_state->parent;

    if (bdb_state->isopen == 1) {
        print(bdb_state, "bdb_open_again_tran(%s) isopen, not opening again\n",
              bdb_state->name);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bdb_state->read_write)
        iammaster = 1;
    else
        iammaster = 0;

    rc = open_dbs(bdb_state, iammaster, 1, 0, tid, flags);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "upgrade: open_dbs as master failed\n");
        BDB_RELLOCK();
        *bdberr = BDBERR_MISC;
        return -1;
    }
    bdb_state->isopen = 1;

    bdb_lock_children_lock(parent);

    /* chain us into a free slot, or extend */
    for (i = 0; i < parent->numchildren; i++) {
        if (parent->children[i] == NULL) {
            parent->children[i] = bdb_state;
            chained = 1;
            ctrace("bdb_open_again_tran took free slot %d\n", i);
            break;
        }
    }
    if (!chained) {
        parent->children[bdb_state->parent->numchildren] = bdb_state;
        ctrace("bdb_open_again_tran took last slot %d and extended\n",
               parent->numchildren);
        parent->numchildren++;
        assert(parent->numchildren < MAX_CHILDREN);
    }

    bdb_unlock_children_lock(parent);

    BDB_RELLOCK();

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_open_again(bdb_state_type *bdb_state, int *bdberr)
{
    DB_TXN *tid;
    int rc;

    rc = bdb_state->dbenv->txn_begin(bdb_state->dbenv, NULL, &tid, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_open_again begin tran failed\n");
        exit(1);
    }

    rc = bdb_open_again_tran_int(bdb_state, tid, 0, bdberr);

    rc = tid->commit(tid, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "bdb_open_again commit tran failed\n");
        exit(1);
    }

    return rc;
}

int bdb_open_again_foreign_bulkimport(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_open_again_tran_int(bdb_state, NULL, BDB_OPEN_SKIP_SCHEMA_LK, bdberr);
}

int bdb_open_again_tran(bdb_state_type *bdb_state, tran_type *tran, int *bdberr)
{
    return bdb_open_again_tran_int(bdb_state, tran ? tran->tid : NULL, 0, bdberr);
}

int bdb_open_again_tran_queue(bdb_state_type *bdb_state, tran_type *tran,
                              uint32_t flags, int *bdberr)
{
    return bdb_open_again_tran_int(bdb_state, tran ? tran->tid : NULL,
                                   flags, bdberr);
}

int bdb_rebuild_done(bdb_state_type *bdb_state)
{
    int rc;

    rc = ll_checkpoint(bdb_state, 1);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "txn_checkpoint err %d\n", rc);
        return -1;
    }

    delete_log_files(bdb_state);

    return 0;
}

static uint64_t mystat(const char *filename)
{
    struct stat st;
    int rc;
    rc = stat(filename, &st);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "cannot stat %s: %d %s\n", filename, errno,
                strerror(errno));
        return 0;
    }
    return st.st_size;
}

uint64_t bdb_index_size_tran(bdb_state_type *bdb_state, tran_type *tran, int ixnum)
{
    char bdbname[PATH_MAX], physname[PATH_MAX];

    if (ixnum < 0 || ixnum >= bdb_state->numix)
        return 0;

    form_indexfile_name(bdb_state, tran ? tran->tid : NULL, ixnum, bdbname, sizeof(bdbname));
    bdb_trans(bdbname, physname);

    return mystat(physname);
}

uint64_t bdb_data_size_tran(bdb_state_type *bdb_state, tran_type *tran, int dtanum)
{
    int stripenum, numstripes = 1;
    uint64_t total = 0;

    if (dtanum < 0 || dtanum >= bdb_state->numdtafiles)
        return 0;

    if (dtanum == 0 || bdb_state->attr->blobstripe)
        numstripes = bdb_state->attr->dtastripe;

    for (stripenum = 0; stripenum < numstripes; stripenum++) {
        char bdbname[PATH_MAX], physname[PATH_MAX];
        form_datafile_name(bdb_state, tran ? tran->tid : NULL, dtanum, stripenum, bdbname, sizeof(bdbname));
        bdb_trans(bdbname, physname);
        total += mystat(physname);
    }

    return total;
}

static size_t dirent_buf_size(const char *dir)
{
    long name_max;
    size_t name_end;
    name_max = pathconf(dir, _PC_NAME_MAX);
    if (name_max <= 255)
        name_max = 255;
    name_end = (size_t)offsetof(struct dirent, d_name) + name_max + 1;
    return (name_end > sizeof(struct dirent) ? name_end
                                             : sizeof(struct dirent));
}

uint64_t bdb_queuedb_size_tran(bdb_state_type *bdb_state, tran_type *tran)
{
    uint64_t totalSize = 0;
    assert(bdb_state->bdbtype == BDBTYPE_QUEUEDB);
    assert(BDB_QUEUEDB_MAX_FILES == 2); // TODO: Hard-coded for now.
    for (int dtanum = 0; dtanum < BDB_QUEUEDB_MAX_FILES; dtanum++) {
        unsigned long long old_qdb_file_ver;
        if (should_stop_looking_for_queuedb_files(bdb_state, tran, dtanum, &old_qdb_file_ver)) {
            break;
        }
        char tmpname[PATH_MAX];
        form_queuedb_name_int(
            bdb_state, tmpname, sizeof(tmpname), old_qdb_file_ver
        );
        char tmpnamenew[PATH_MAX];
        struct stat st;
        int rc = stat(bdb_trans(tmpname, tmpnamenew), &st);
        if (rc == 0) {
            totalSize += st.st_size;
        } else {
            logmsg(LOGMSG_ERROR, "%s: stat(%s) rc %d\n",
                   __func__, tmpname, rc);
        }
    }
    return totalSize;
}

uint64_t bdb_queuedb_size(bdb_state_type *bdb_state)
{
    return bdb_queuedb_size_tran(bdb_state, NULL);
}

uint64_t bdb_queue_size_tran(bdb_state_type *bdb_state, tran_type *tran, unsigned *num_extents)
{
    DIR *dh;
    struct dirent *dirent_buf;
    struct dirent *result;
    char extent_prefix[128];
    int prefix_len;
    uint64_t total = 0;
    bdb_state_type *bdb_env;

    if (bdb_state->parent)
        bdb_env = bdb_state->parent;
    else
        bdb_env = bdb_state;

    *num_extents = 0;

    if (bdb_state->bdbtype == BDBTYPE_QUEUEDB)
        return bdb_queuedb_size_tran(bdb_state, tran);

    prefix_len = snprintf(extent_prefix, sizeof(extent_prefix),
                          "__dbq.%s.queue.", bdb_state->name);

    /* Scan the environment directory for queue extents */
    dh = opendir(bdb_env->txndir);
    if (!dh) {
        logmsg(LOGMSG_ERROR, "%s: opendir error on %s: %d %s\n", __func__,
                bdb_env->txndir, errno, strerror(errno));
        return 0;
    }

    dirent_buf = alloca(dirent_buf_size(bdb_env->txndir));

    while (bb_readdir(dh, dirent_buf, &result) == 0 && result) {
        if (strncmp(result->d_name, extent_prefix, prefix_len) == 0) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s", bdb_env->txndir,
                     result->d_name);
            total += mystat(path);
            (*num_extents)++;
        }
    }

    closedir(dh);
    return total;
}

uint64_t bdb_logs_size(bdb_state_type *bdb_state, unsigned *num_logs)
{
    DIR *dh;
    struct dirent *dirent_buf;
    struct dirent *result;
    uint64_t total = 0;
    bdb_state_type *bdb_env;
    int ii;

    if (bdb_state->parent)
        bdb_env = bdb_state->parent;
    else
        bdb_env = bdb_state;

    *num_logs = 0;

    /* Scan the environment directory for queue extents */
    dh = opendir(bdb_env->txndir);
    if (!dh) {
        logmsg(LOGMSG_ERROR, "%s: opendir error on %s: %d %s\n", __func__,
                bdb_env->txndir, errno, strerror(errno));
        return 0;
    }

    dirent_buf = alloca(dirent_buf_size(bdb_env->txndir));

    while (bb_readdir(dh, dirent_buf, &result) == 0 && result) {
        /* Match log.########## (log. followed by 10 digits) */
        if (strncmp(result->d_name, "log.", 4) == 0 &&
            strlen(result->d_name) == 14) {
            for (ii = 4; ii < 14; ii++)
                if (!isdigit(result->d_name[ii]))
                    break;
            if (ii == 14) {
                char path[PATH_MAX];
                snprintf(path, sizeof(path), "%s/%s", bdb_env->txndir,
                         result->d_name);
                total += mystat(path);
                (*num_logs)++;
            }
        }
    }

    closedir(dh);
    return total;
}

uint64_t bdb_tmp_size(bdb_state_type *bdb_state, uint64_t *ptmptbls, uint64_t *psqlsorters, uint64_t *pblkseqs,
                      uint64_t *pothers)
{
    DIR *dh;
    struct dirent *dirent_buf;
    struct dirent *result;
    uint64_t total = 0;
    bdb_state_type *bdb_env;

    char path[PATH_MAX];
    uint64_t fsz;
    uint64_t tmptbls = *ptmptbls = 0;
    uint64_t sqlsorters = *psqlsorters = 0;
    uint64_t blkseqs = *pblkseqs = 0;
    uint64_t others = *pothers = 0;

    if (bdb_state->parent)
        bdb_env = bdb_state->parent;
    else
        bdb_env = bdb_state;

    /* Scan the environment directory for queue extents */
    dh = opendir(bdb_env->tmpdir);
    if (!dh) {
        logmsg(LOGMSG_ERROR, "%s: opendir error on %s: %d %s\n", __func__, bdb_env->tmpdir, errno, strerror(errno));
        return 0;
    }

    dirent_buf = alloca(dirent_buf_size(bdb_env->tmpdir));

    while (bb_readdir(dh, dirent_buf, &result) == 0 && result) {
        if (strcmp(result->d_name, ".") == 0 || strcmp(result->d_name, "..") == 0)
            continue;

        snprintf(path, sizeof(path), "%s/%s", bdb_env->tmpdir, result->d_name);
        fsz = mystat(path);

        if (strncmp(result->d_name, "_temp_", 6) == 0)
            tmptbls += fsz;
        else if (strncmp(result->d_name, "sqlsort_", 8) == 0)
            sqlsorters += fsz;
        else if (strncmp(result->d_name, "_blkseq", 7) == 0)
            blkseqs += fsz;
        else
            others += fsz;

        total += fsz;
    }

    closedir(dh);

    *ptmptbls = tmptbls;
    *psqlsorters = sqlsorters;
    *pblkseqs = blkseqs;
    *pothers = others;

    return total;
}

void bdb_log_berk_tables(bdb_state_type *bdb_state)
{
    __bb_dbreg_print_dblist(bdb_state->dbenv,
                            (void (*)(void *, const char *, ...))print,
                            bdb_state);
}

int bdb_get_data_filename(bdb_state_type *bdb_state, int stripe, int blob,
                          char *nameout, int namelen, int *bdberr)
{
    int rc;
    *bdberr = BDBERR_NOERROR;
    rc = form_datafile_name(bdb_state, NULL, blob, stripe, nameout, namelen);
    if (rc >= 0)
        rc = 0;
    else
        rc = -1;
    return rc;
}

int bdb_get_index_filename(bdb_state_type *bdb_state, int ixnum, char *nameout,
                           int namelen, int *bdberr)
{
    int rc;

    *bdberr = BDBERR_NOERROR;
    if (ixnum == -1) {
        /* don't need data files yet, so not supported yet */
        *bdberr = BDBERR_BADARGS;
        rc = -1;
    } else {
        rc = form_indexfile_name(bdb_state, NULL, ixnum, nameout, namelen);
        if (rc > 0)
            rc = 0;
    }
    return rc;
}

int __dbreg_exists(DB_ENV *dbenv, const char *find_name);

void bdb_verify_dbreg(bdb_state_type *bdb_state)
{
    bdb_state_type *s;
    int tbl, blob, stripe, ix;
    char fname[255];
    int exists;

    bdb_lock_children_lock(bdb_state);

    for (tbl = 0; tbl < bdb_state->numchildren; tbl++) {
        s = bdb_state->children[tbl];
        if (s) {
            if (s->bdbtype == BDBTYPE_TABLE) {
                for (blob = 0; blob < s->numdtafiles; blob++) {
                    int nstripes;
                    if (blob == 0)
                        nstripes = bdb_state->attr->dtastripe;
                    else
                        nstripes = bdb_state->attr->blobstripe
                                       ? bdb_state->attr->dtastripe
                                       : 1;

                    for (stripe = 0; stripe < nstripes; stripe++) {
                        form_datafile_name(s, NULL, blob, stripe, fname,
                                           sizeof(fname));
                        exists = __dbreg_exists(bdb_state->dbenv, fname);
                        if (!exists)
                            logmsg(LOGMSG_WARN, "no dbreg entries for %s dta %d stripe %d\n",
                                   s->name, blob, stripe);
                    }
                }
                for (ix = 0; ix < s->numix; ix++) {
                    form_indexfile_name(s, NULL, ix, fname, sizeof(fname));
                    exists = __dbreg_exists(bdb_state->dbenv, fname);
                    if (!exists)
                        logmsg(LOGMSG_WARN, "no dbreg entries for %s ix %d\n", s->name, ix);
                }
            }
        }
    }

    bdb_unlock_children_lock(bdb_state);
}

void bdb_set_origname(bdb_state_type *bdb_state, const char *name)
{
    if (bdb_state->origname) {
        free(bdb_state->origname);
        bdb_state->origname = NULL;
    }
    bdb_state->origname = strdup(name);
}

void bdb_genid_sanity_check(bdb_state_type *bdb_state, unsigned long long genid,
                            int stripe)
{
    time_t time;
    struct tm tm;
    unsigned long long fgenid, normgenid;
    fgenid = flibc_ntohll(genid);

    time = bdb_genid_timestamp(genid);
    localtime_r(&time, &tm);
    if (1900 + tm.tm_year < 2000)
        logmsg(LOGMSG_WARN, "%s: %s stripe %d suspiciously old genid: %016llx "
                        "(%02d/%02d/%04d %02d:%02d:%02d)\n",
                __func__, bdb_state->name, stripe, genid, tm.tm_mon + 1,
                tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour, tm.tm_min,
                tm.tm_sec);
    normgenid = bdb_normalise_genid(bdb_state, genid);
    if (normgenid != genid)
        logmsg(LOGMSG_WARN, "%s: %s stripe %d old style genid %016llx "
                        "(%02d/%02d/%04d %02d:%02d:%02d) \n",
                __func__, bdb_state->name, stripe, fgenid, tm.tm_mon + 1,
                tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour, tm.tm_min,
                tm.tm_sec);
}

struct unused_file {
    unsigned lognum;
    char fname[1];
};

#define OF_LIST_MAX 16384
hash_t *oldfile_hash;
static struct unused_file *of_list[OF_LIST_MAX];
static int list_hd = 0, list_tl = 0;
static pthread_mutex_t unused_files_mtx = PTHREAD_MUTEX_INITIALIZER;

static int oldfile_hash_dump(void *ptr, void *unused)
{
    struct unused_file *obj = ptr;
    logmsg(LOGMSG_USER, "%s: str='%s' %d (obj %p)\n", __func__, obj->fname,
           obj->lognum, obj);
    return 0;
}

void oldfile_dump(void)
{
    if (oldfile_hash == NULL) {
        logmsg(LOGMSG_USER, "%s: oldfile hash is NULL\n", __func__);
        return;
    }
    hash_for(oldfile_hash, oldfile_hash_dump, NULL);
}

/* use hash tbl to check if we have added file to list previously */
static int oldfile_list_contains(const char *filename)
{
    struct unused_file *ptr = NULL;
    Pthread_mutex_lock(&unused_files_mtx);
    if (oldfile_hash) {
        ptr = hash_find_readonly(oldfile_hash, filename);
    }
    Pthread_mutex_unlock(&unused_files_mtx);

#ifdef DEBUG
    if (ptr)
        logmsg(LOGMSG_INFO, "tid: 0x%x found in oldfilelist file %s\n",
                pthread_self(), filename);
#endif
    return ptr != NULL;
}


/* Add ptr to hash and to list
 * if filename already in hash or if hash is full or if could not add to hash,
 * this function will return nonzero rc, and in that case caller needs
 * to free ptr accordingly.
 */
static int oldfile_add_ptr(struct unused_file *ptr, const char *func, int line,
                           int spew_debug)
{
    int rc = 0;
    if (!ptr)
        return 1;

    Pthread_mutex_lock(&unused_files_mtx);
    if (!oldfile_hash)
        oldfile_hash = hash_init_str(offsetof(struct unused_file, fname));

    if (hash_get_num_entries(oldfile_hash) >= OF_LIST_MAX - 1) {
        logmsg(LOGMSG_ERROR, "Failed to add %s, (%d,%d)\n", ptr->fname, list_hd,
               list_tl);
        rc = 1;
        goto done; // nospace
    }

    if (hash_find_readonly(oldfile_hash, ptr->fname) != NULL) { //check under lock before adding
        rc = 1;
        goto done; // already in hash
    }

    rc = hash_add(oldfile_hash, ptr);

    if (!rc) {
        assert ((list_hd + 1) % OF_LIST_MAX != list_tl);
        of_list[list_hd] = ptr;
        list_hd++;
        list_hd %= OF_LIST_MAX;
        if (spew_debug)
            logmsg(LOGMSG_DEBUG,
                   "%s:%d [%s] list_hd %d, list_tl %d from %s:%d\n", __func__,
                   __LINE__, ptr->fname, list_hd, list_tl, func, line);
    }

done:
    Pthread_mutex_unlock(&unused_files_mtx);
    return rc;
}

static int oldfile_add(const char *filename, int lognum, const char *func,
                       int line, int spew_debug)
{
    size_t len = strlen(filename);
    struct unused_file *ptr = malloc(sizeof(struct unused_file) + len);
    if (!ptr)
        return 1;
    ptr->lognum = lognum;
    memcpy(ptr->fname, filename, len + 1);

    int rc = oldfile_add_ptr(ptr, func, line, spew_debug);
    if (rc) {
        free(ptr);
    }
    return rc;
}

/* Get first oldfile in list and remove from both list and hash 
 * Caller should free the returned pointer */
static struct unused_file *oldfile_get_first(int spew_debug)
{
    struct unused_file *ptr = NULL;
    Pthread_mutex_lock(&unused_files_mtx);

    if (list_tl != list_hd) {
        ptr = of_list[list_tl];
        of_list[list_tl] = NULL;
        list_tl = (list_tl + 1) % OF_LIST_MAX;
        if (spew_debug)
            logmsg(LOGMSG_DEBUG, "%s:%d [%s] list_hd %d, list_tl %d\n",
                   __func__, __LINE__, ptr->fname, list_hd, list_tl);

        assert(hash_find(oldfile_hash, ptr->fname) == ptr);
        hash_del(oldfile_hash, ptr);
    }

    Pthread_mutex_unlock(&unused_files_mtx);
    return ptr;
}

static int free_unused_file(void *obj, void *arg)
{
    free(obj);
    return 0;
}

void oldfile_clear(void)
{
    Pthread_mutex_lock(&unused_files_mtx);

    list_hd = 0;
    list_tl = 0;

    if (oldfile_hash) {
        hash_for(oldfile_hash, free_unused_file, NULL);
        hash_clear(oldfile_hash);
        hash_free(oldfile_hash);
        oldfile_hash = NULL;
    }
    Pthread_mutex_unlock(&unused_files_mtx);
}

int oldfile_is_empty(void)
{
    int ret = 1;
    Pthread_mutex_lock(&unused_files_mtx);
    if (list_tl != list_hd) {
        ret = 0;
        assert(oldfile_hash);
        assert(hash_get_num_entries(oldfile_hash) != 0);
    }
    else {
        assert(oldfile_hash == NULL || hash_get_num_entries(oldfile_hash) == 0);
    }
    Pthread_mutex_unlock(&unused_files_mtx);

    return ret;
}

int bdb_get_logfile(bdb_state_type *bdb_state, int *bdberr, int which)
{
    DB_LOGC *logc;
    DBT logent;
    DB_LSN current_lsn;
    int rc;
    int lognum;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: create log cursor rc %d\n", __func__, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    bzero(&logent, sizeof(DBT));
    logent.flags = DB_DBT_MALLOC;
    rc = logc->get(logc, &current_lsn, &logent, which);
    if (rc) {
        logc->close(logc, 0);
        logmsg(LOGMSG_ERROR, "%s: logc->get last LSN rc %d\n", __func__, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    if (logent.data)
        free(logent.data);

    lognum = current_lsn.file;
    logc->close(logc, 0);

    return lognum;
}

int bdb_get_first_logfile(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_get_logfile(bdb_state, bdberr, DB_FIRST);
}

int bdb_get_last_logfile(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_get_logfile(bdb_state, bdberr, DB_LAST);
}

/* Lets check new prefix for ongoing schema changes:
 * This is a bit of a kludge. We use the first 32 bytes of the table
 * name as a key into llmeta. See bdb_open_int().
 *
 * Return 1 if file_version is currently being used as new.tblname
 *
 * NOTE:
    When we check llmeta to determine if a file can be deleted,
    we have to check new. prefix for schemachange first because
    we are not reading llmeta transactionally and ongoing schema
    change can commit at any time.
 *
 * Example:
 * By checking new. prefix first, we have:
 * 1. check is_table_in_schema_change
 * 2. check llmeta for new.tblname
 * 3. check llmeta for tblname
 *
 * Then if an onging schema change commits/aborts before 1,
 * we can delete any files not found in step 3. Else if an ongoing
 * schema change commits before 2 (after 1), step 3 will see
 * the committed file verison in llmeta (i.e. don't delete). If the
 * schema change commits/aborts after 2, step 2 would have returned
 * found already and the unused files will be handled in the next run.
 *
 * Counterexample:
 * If we don't check new. prefix first, we would have:
 * 1. check llmeta for tblname
 * 2. check is_table_in_schema_change
 * 3. check llmeta for new.tblname
 *
 * Then if an ongoing schema change commits between 2 and 3,
 * we will end up deleting a wrong file as in this case
 * neither step 1 nor 3 would find the file version in llmeta.
 */
static inline int bdb_is_new_sc_file(bdb_state_type *bdb_state, tran_type *tran,
                                     const char *tblname,
                                     unsigned long long version, int *bdberr)
{
    int rc = 0;
    char newname[32] = {0}; // LLMETA_TBLLEN = 32
    snprintf(newname, 32, "%s%s", NEW_PREFIX, tblname);

    uint32_t lid = 0;
    int islocked = 0;
    if (tran) {
        bdb_get_tran_lockerid(tran, &lid);
        islocked = bdb_has_tablename_locked(bdb_state, tblname, lid, TABLENAME_LOCKED_EITHER);
    }
    if (!islocked) {
        rc = is_table_in_schema_change(tblname, tran);
        if (rc == 0) // table not in_schema_change
            return 0;
        else if (rc < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d failed to check in_schema_change for %s\n",
                    __func__, __LINE__, tblname);
            *bdberr = BDBERR_MISC;
            return -1;
        }
    }

    rc = bdb_process_each_table_dta_entry(bdb_state, tran, newname, version,
                                          bdberr);
    if (rc == 1)
        return 1;

    rc = bdb_process_each_table_idx_entry(bdb_state, tran, newname, version,
                                          bdberr);
    if (rc == 1)
        return 1;

    return rc;
}

static int bdb_get_lowfilenum (bdb_state_type *bdb_state, int *bdberr) {
    int lognum, ourlowfilenum;

    /* if there's no cluster, use our log file, otherwise use the cluster
     * low watermark,
     * or our low watermark, whichever is lower */
    lognum = get_lowfilenum_sanclist(bdb_state);

    ourlowfilenum = bdb_get_first_logfile(bdb_state, bdberr);
    if (ourlowfilenum == -1) return -1;
    if (lognum == 0) lognum = ourlowfilenum;

    if (ourlowfilenum < lognum) lognum = ourlowfilenum;
    return lognum;
}

/* check if any of the file on disks start with tblname, and have the known name
   format
   (depending on file type), and if they are, queue them to the to-be-deleted
   file list
 */
int bdb_check_files_on_disk(bdb_state_type *bdb_state, const char *tblname, int *bdberr)
{
    int spew_debug =
        bdb_attr_get(bdb_state->attr, BDB_ATTR_DELETE_OLD_FILE_DEBUG);
    const char data_ext[] = ".data";
    const char index_ext[] = ".index";
    const char blob_ext[] = ".blob";
    int rc = 0;
    char table_prefix[80];
    unsigned long long file_version;

    struct dirent *buf;
    struct dirent *ent;
    DIR *dirp;
    int error;
    int lognum = 0;

    assert(bdb_state->parent == NULL);

    if (!bdb_state || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: null or invalid argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* must be large enough to hold a dirent struct with the longest possible
     * filename */
    buf = malloc(bb_dirent_size(bdb_state->dir));
    if (!buf) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    /* open the db's directory */
    dirp = opendir(bdb_state->dir);
    if (!dirp) {
        logmsg(LOGMSG_ERROR, "%s: opendir failed\n", __func__);
        *bdberr = BDBERR_MISC;
        free(buf);
        return -1;
    }

    int tp_len = snprintf(table_prefix, sizeof(table_prefix), "%s_", tblname);
    if (tp_len >= sizeof(table_prefix)) {
        logmsg(LOGMSG_ERROR, "%s: tablename too long\n", __func__);
        *bdberr = BDBERR_MISC;
        free(buf);
        closedir(dirp);
        return -1;
    }

    /* for each file in the db's directory */
    while ((error = bb_readdir(dirp, buf, &ent)) == 0 && ent != NULL) {
        /* if the file's name is longer then the prefix and it belongs to our
         * table */
        if (!(strlen(ent->d_name) > tp_len &&
              strncmp(ent->d_name, table_prefix, tp_len) == 0))
            continue;

        const char *file_name_post_prefix;
        unsigned long long invers;
        char *endp;

        /* file version should start right after the prefix */
        file_name_post_prefix = ent->d_name + tp_len;

        /* try to parse a file version */
        invers = strtoull(file_name_post_prefix, &endp, 16 /*base*/);

        /* if no file_version was found after the prefix or the next thing
         * after the file version isn't .blob or .data or .index */
        if (endp == file_name_post_prefix ||
            (strncmp(endp, data_ext, sizeof(data_ext) - 1) != 0 &&
             strncmp(endp, index_ext, sizeof(index_ext) - 1) != 0 &&
             strncmp(endp, blob_ext, sizeof(blob_ext) - 1) != 0)) {
            file_version = 0;
        } else {
            uint8_t *p_buf = (uint8_t *)&invers,
                    *p_buf_end = p_buf + sizeof(invers);
            struct bdb_file_version_num_type p_file_version_num_type;

            bdb_file_version_num_get(&p_file_version_num_type, p_buf,
                                     p_buf_end);

            file_version = p_file_version_num_type.version_num;
        }

        /*fprintf(stderr, "found version %s %016llx on disk\n",*/
        /*ent->d_name, file_version); */

        if (!file_version)
            continue;

        /* brute force scan to find any files on disk that we aren't
         * actually using */
        int found_in_llmeta = 0;

        /* We have to check new. prefix for schemachange first.
         * See NOTE in bdb_is_new_sc_file()
         */
        rc = bdb_is_new_sc_file(bdb_state, NULL, tblname, file_version, bdberr);
        if (rc == 1) {
            found_in_llmeta = 1;
            rc = 0;
        } else if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d failed to check llmeta for %s, rc %d, bdberr %d\n",
                   __func__, __LINE__, ent->d_name, rc, *bdberr);
            continue;
        }

        if (!found_in_llmeta) {
            rc = bdb_process_each_table_dta_entry(bdb_state, NULL, tblname,
                                                  file_version, bdberr);
            if (rc == 1) {
                found_in_llmeta = 1;
                rc = 0;
            } else if (rc) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d failed to check llmeta for %s, rc %d, bdberr %d\n",
                       __func__, __LINE__, ent->d_name, rc, *bdberr);
                continue;
            }
        }

        if (!found_in_llmeta) {
            rc = bdb_process_each_table_idx_entry(bdb_state, NULL, tblname,
                                                  file_version, bdberr);
            if (rc == 1) {
                found_in_llmeta = 1;
                rc = 0;
            } else if (rc) {
                logmsg(LOGMSG_ERROR,
                       "%s:%d failed to check llmeta for %s, rc %d, bdberr %d\n",
                       __func__, __LINE__, ent->d_name, rc, *bdberr);
                continue;
            }
        }

        if (found_in_llmeta)
            continue;

        /* if the file's version wasn't found in llmeta, delete it */
        char munged_name[FILENAMELEN];

        if (snprintf(munged_name, sizeof(munged_name), "XXX.%s", ent->d_name) >=
            sizeof(munged_name)) {
            logmsg(LOGMSG_ERROR, "%s: filename too long to munge: %s\n",
                   __func__, ent->d_name);
            continue;
        }

        /* dont add filename more than once in the list */
        if (oldfile_list_contains(munged_name))
            continue;

        /* Always use most recent logfile when placing on oldfile list.
           Refresh lognum because it may have pushed forward while
           we scan the files. */
        if (bdb_state->attr->keep_referenced_files) {
            lognum = bdb_get_last_logfile(bdb_state, bdberr);
            if (lognum < 0)
                return -1;
        }

        if (oldfile_add(munged_name, lognum, __func__, __LINE__, spew_debug)) {
            print(bdb_state, "failed to add old file to hash: %s\n", ent->d_name);
            break;
        } else {
            if (spew_debug)
                logmsg(LOGMSG_DEBUG, "%s: requeued file %s\n", __func__,
                       ent->d_name);
            print(bdb_state, "requeued old file %s\n", ent->d_name);
        }
    }

    closedir(dirp);
    free(buf);
    *bdberr = BDBERR_NOERROR;
    return 0;
}


static inline void init_version_num(unsigned long long *version_num, int sz) {
    for(int i = 0; i < sz; i++)
        version_num[i] = -1;
}

/* given an existing table pointed by bdb_state, check the disk for older
   versions of it
   (i.e. not matching current metadata versioning information), and queue those
   for deletion
   NOTE: obviously, this is only working for recovering to-be-deleted files
   during
   alter and fastinit process
*/
static int bdb_process_unused_files(bdb_state_type *bdb_state, tran_type *tran,
                                    int *bdberr, char *powner, int delay)
{
    int spew_debug =
        bdb_attr_get(bdb_state->attr, BDB_ATTR_DELETE_OLD_FILE_DEBUG);
    static char *owner = NULL;
    static pthread_mutex_t owner_mtx = PTHREAD_MUTEX_INITIALIZER;
    const char blob_ext[] = ".blob";
    const char data_ext[] = ".data";
    const char index_ext[] = ".index";
    const char qdb_ext[] = ".queuedb";
    int rc = 0;
    char table_prefix[80];
    unsigned long long file_version;
    unsigned long long dta_version_num[bdb_state->numdtafiles];
    unsigned long long idx_version_num[bdb_state->numix];

    struct dirent *buf;
    struct dirent *ent;
    DIR *dirp;
    int error;
    int lognum = 0;

    int delay_delete = delay && bdb_state->attr->keep_referenced_files;

    assert(bdb_state->parent != NULL);

    if (!bdb_state || !bdberr) {
        logmsg(LOGMSG_ERROR, "%s: null or invalid argument\n", __func__);
        if (bdberr)
            *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (!bdb_have_llmeta()) {
        logmsg(LOGMSG_ERROR, "%s: db is not llmeta\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    Pthread_mutex_lock(&owner_mtx);
    if (owner != NULL) {
        logmsg(LOGMSG_INFO, "Old file deletion in progress(\"%s\")\n", owner);
        Pthread_mutex_unlock(&owner_mtx);
        return 0;
    } else {
        owner = powner;
    }
    Pthread_mutex_unlock(&owner_mtx);

    int tp_len =
        snprintf(table_prefix, sizeof(table_prefix), "%s_", bdb_state->name);
    if (tp_len >= sizeof(table_prefix)) {
        logmsg(LOGMSG_ERROR, "%s: tablename too long\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    /* must be large enough to hold a dirent struct with the longest possible
     * filename */
    buf = alloca(bb_dirent_size(bdb_state->parent->dir));

    /* open the db's directory */
    dirp = opendir(bdb_state->parent->dir);
    if (!dirp) {
        logmsg(LOGMSG_ERROR, "%s: opendir failed\n", __func__);
        *bdberr = BDBERR_MISC;
        return -1;
    }

    // cache version number in these arrays
    init_version_num(dta_version_num, bdb_state->numdtafiles);
    init_version_num(idx_version_num, bdb_state->numix);

    /* for each file in the db's directory */
    while ((error = bb_readdir(dirp, buf, &ent)) == 0 && ent != NULL) {
        /* if the file's name is longer then the prefix and it belongs to our
         * table */
        if (!(strlen(ent->d_name) > tp_len &&
              strncmp(ent->d_name, table_prefix, tp_len) == 0))
            continue;

        const char *file_name_post_prefix;
        unsigned long long invers;
        char *endp;

        /* file version should start right after the prefix */
        file_name_post_prefix = ent->d_name + tp_len;

        /* try to parse a file version */
        invers = strtoull(file_name_post_prefix, &endp, 16 /*base*/);

        /* if no file_version was found after the prefix or the next thing
         * after the file version isn't .blob or .data or .index */
        if (endp == file_name_post_prefix ||
            (strncmp(endp, blob_ext, sizeof(blob_ext) - 1) != 0 &&
             strncmp(endp, data_ext, sizeof(data_ext) - 1) != 0 &&
             strncmp(endp, qdb_ext, sizeof(qdb_ext) - 1) != 0 &&
             strncmp(endp, index_ext, sizeof(index_ext) - 1) != 0)) {
            file_version = 0;
        } else {
            uint8_t *p_buf = (uint8_t *)&invers,
                    *p_buf_end = p_buf + sizeof(invers);
            struct bdb_file_version_num_type p_file_version_num_type;

            bdb_file_version_num_get(&p_file_version_num_type, p_buf,
                                     p_buf_end);

            file_version = p_file_version_num_type.version_num;
        }

        if (!file_version)
            continue;

        /* brute force scan to find any files on disk that we aren't
         * actually using */
        int found_in_llmeta = 0;
        int i;

        /* We have to check new. prefix for schemachange first.
         * See NOTE in bdb_is_new_sc_file()
         */
        /* Core is showing bdb_is_new_sc_file has tblname 0xffffffffffffffff ..
         * maybe this is a race? */

        if (bdb_state->name == (char *)0xffffffffffffffff)
            abort();

        rc = bdb_is_new_sc_file(bdb_state, tran, (const char *)bdb_state->name,
                                file_version, bdberr);
        if (rc == 1) {
            found_in_llmeta = 1;
            rc = 0;
        } else if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d failed to check llmeta for %s, rc %d, bdberr %d\n",
                   __func__, __LINE__, ent->d_name, rc, *bdberr);
            continue;
        }

        /* try to find the file version amongst the active data files */
        for (i = 0; i < bdb_state->numdtafiles; ++i) {
            if (dta_version_num[i] == -1) {
                if (bdb_state->bdbtype == BDBTYPE_QUEUEDB) {
                    rc = bdb_get_file_version_qdb(bdb_state, tran, i, &dta_version_num[i], bdberr);
                } else {
                    rc = bdb_get_file_version_data(bdb_state, tran, i, &dta_version_num[i], bdberr);
                }

                if (rc == 1) {
                    /* table doesnt exist in llmeta, not an error */
                    *bdberr = BDBERR_NOERROR;
                    rc = 0;
                } else if (rc != 0)
                    break;
            }

            if (dta_version_num[i] == file_version) {
                found_in_llmeta = 1;
                break;
            }
        }

        /* try to find the file version amongst the active indices */
        for (i = 0; !rc && !found_in_llmeta && i < bdb_state->numix; ++i) {
            if (bdb_state->bdbtype == BDBTYPE_QUEUEDB)
                break;
            if (idx_version_num[i] == -1) {
                rc = bdb_get_file_version_index(bdb_state, tran, i /*dtanum*/,
                                                &idx_version_num[i], bdberr);
                if (rc == 1) {
                    /* table doesnt exist in llmeta, not an error */
                    *bdberr = BDBERR_NOERROR;
                    rc = 0;
                } else if (rc != 0)
                    break;
            }

            if (idx_version_num[i] == file_version) {
                found_in_llmeta = 1;
                break;
            }
        }

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d failed to check llmeta for %s, rc %d, bdberr %d\n",
                   __func__, __LINE__, ent->d_name, rc, *bdberr);
            continue;
        }

        if (found_in_llmeta)
            continue;

        /* if the file's version wasn't found in llmeta, delete it */
        char munged_name[FILENAMELEN];

        if (snprintf(munged_name, sizeof(munged_name), "XXX.%s", ent->d_name) >=
            sizeof(munged_name)) {
            logmsg(LOGMSG_ERROR, "%s: filename too long to munge: %s\n",
                   __func__, ent->d_name);
            continue;
        }

        if (delay) {
            /* dont add filename more than once in the list */
            if (oldfile_list_contains(munged_name))
                continue;

            /* Always use most recent logfile when placing on oldfile list.
               Refresh lognum because it may have pushed forward while
               we scan the files. */
            if (delay_delete) {
                lognum = bdb_get_last_logfile(bdb_state, bdberr);
                if (lognum < 0)
                    return -1;
            }

            if (oldfile_add(munged_name, lognum, __func__, __LINE__,
                            spew_debug)) {
                print(bdb_state, "failed to collect old file %s\n", ent->d_name);
                break;
            } else {
                print(bdb_state, "collected old file %s\n", ent->d_name);
            }
        } else {
            logmsg(LOGMSG_INFO, "%s: deleting file %s\n", __func__, ent->d_name);
            print(bdb_state, "deleting file %s\n", ent->d_name);
            DB_TXN *tid;
            if (bdb_state->dbenv->txn_begin(bdb_state->dbenv,
                                            tran ? tran->tid : NULL, &tid,
                                            0 /*flags*/)) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to begin trans for deleteing file: %s\n",
                       __func__, ent->d_name);
                continue;
            }
            if (bdb_del_file(bdb_state, tid, munged_name, bdberr)) {
                logmsg(LOGMSG_ERROR, "%s: failed to delete file: %s\n",
                       __func__, ent->d_name);
                tid->abort(tid);
            } else if (tid->commit(tid, 0)) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to commit trans for deleteing file: %s\n",
                       __func__, ent->d_name);
            }
        }
    }

    closedir(dirp);

    Pthread_mutex_lock(&owner_mtx);
    owner = NULL;
    Pthread_mutex_unlock(&owner_mtx);

    *bdberr = BDBERR_NOERROR;
    return 0;
}

int bdb_del_unused_files_tran(bdb_state_type *bdb_state, tran_type *tran,
                              int *bdberr)
{
    return bdb_process_unused_files(bdb_state, tran, bdberr, "del_unused", 0);
}

int bdb_del_unused_files(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_del_unused_files_tran(bdb_state, NULL, bdberr);
}

int bdb_list_unused_files_tran(bdb_state_type *bdb_state, tran_type *tran,
                               int *bdberr, char *powner)
{
    return bdb_process_unused_files(bdb_state, tran, bdberr, powner, 1);
}

int bdb_list_unused_files(bdb_state_type *bdb_state, int *bdberr, char *powner)
{
    return bdb_list_unused_files_tran(bdb_state, NULL, bdberr, powner);
}

int bdb_have_unused_files(void) { return oldfile_is_empty() != 1; }

int bdb_purge_unused_files(bdb_state_type *bdb_state, tran_type *tran,
                           int *bdberr)
{
    int spew_debug =
        bdb_attr_get(bdb_state->attr, BDB_ATTR_DELETE_OLD_FILE_DEBUG);
    int rc;
    unsigned lowfilenum = 0;
    struct stat sb;

    /* Purge if cluster low-logfile is larger */
    if (bdb_state->attr->keep_referenced_files) {
        lowfilenum = bdb_get_lowfilenum(bdb_state, bdberr);
        if (lowfilenum < 0)
            return -1;
    }

    *bdberr = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_write_preamble(bdb_state, bdberr)) {
        return -1;
    }

    assert(tran);

    struct unused_file *uf_ptr = oldfile_get_first(spew_debug);

    /* wait some more */
    if (!uf_ptr) return 1;

    /* skip already deleted files */
    char path[PATH_MAX];
    bdb_trans(uf_ptr->fname, path);
    if (stat(path, &sb)) {
        free(uf_ptr);
        return 0;
    }

    if (uf_ptr->lognum && lowfilenum && uf_ptr->lognum >= lowfilenum) {
        if (oldfile_add_ptr(uf_ptr, __func__, __LINE__, spew_debug))
            free(uf_ptr); /* failed to add back so need to free */
        return 1;
    }

    logmsg(LOGMSG_INFO, "deleting file %s\n", uf_ptr->fname);
    print(bdb_state, "deleting file %s\n", uf_ptr->fname);

    if ((rc = bdb_del_file(bdb_state, tran->tid, uf_ptr->fname, bdberr))) {
        logmsg(LOGMSG_ERROR, "%s: failed to delete file rc %d bdberr %d: %s\n",
                __func__, rc, *bdberr, uf_ptr->fname);

        if (*bdberr == BDBERR_DELNOTFOUND)
            rc = 0;
        else if (oldfile_add_ptr(uf_ptr, __func__, __LINE__, spew_debug))
            print(bdb_state,
                  "bdb_del_file failed bdberr=%d and failed to requeue \"%s\"\n",
                  *bdberr, uf_ptr->fname);
        else /* Added back, dont free uf_ptr */
            return rc;
    }

    free(uf_ptr);
    return rc;
}

/* Refactor to not access berkley while holding children lock */
int bdb_osql_cache_table_versions(bdb_state_type *bdb_state, tran_type *tran,
                                  int trak, int *bdberr)
{
    int i = 0;
    int retry;
    char **tablenames;
    int tablecount;
    int rc;

retry:
    rc = retry = 0;
    tablenames = NULL;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (tran->table_version_cache) {
        tran->table_version_cache_sz = 0;
        free(tran->table_version_cache);
        tran->table_version_cache = NULL;
    }

    bdb_lock_children_lock(bdb_state);
    tran->table_version_cache_sz = tablecount = bdb_state->numchildren;
    tablenames = (char **)calloc(sizeof(char *), tablecount);
    tran->table_version_cache =
        (unsigned long long *)calloc(tablecount, sizeof(unsigned long long));

    for (int i = 0; i < tablecount; i++) {
        if (bdb_state->children[i]) {
            tablenames[i] = strdup(bdb_state->children[i]->name);
            tran->table_version_cache[i] = bdb_state->children[i]->version_num;
        }
    }

    bdb_unlock_children_lock(bdb_state);

    if (!tran->table_version_cache) {
        logmsg(LOGMSG_ERROR, "%s: failed to allocated %zu bytes\n", __func__,
               sizeof(unsigned long long) * tran->table_version_cache_sz);
        *bdberr = BDBERR_MALLOC;
        rc = -1;
        goto done;
    }

    for (i = 0; i < tran->table_version_cache_sz; i++) {
        if (tablenames[i] == NULL)
            continue;
        if (tran->table_version_cache[i] == 0) {
            /* read it */
            rc = bdb_get_file_version_data_by_name(
                NULL, tablenames[i], 0, &tran->table_version_cache[i], bdberr);
            if (rc) {
                if (*bdberr == BDBERR_FETCH_DTA) {
                    rc = 0;
                    *bdberr = BDBERR_NOERROR;
                    tran->table_version_cache[i] = -1;
                } else {
                    logmsg(LOGMSG_ERROR, "%s: failed to read file version number "
                                    "rc=%d bdberr=%d\n",
                            __func__, rc, *bdberr);
                    free(tran->table_version_cache);
                    tran->table_version_cache = NULL;
                    tran->table_version_cache_sz = 0;
                    goto done;
                }
            } else {
                /*printf("Caching %s %llx\n", bdb_state->children[i]->name,
                 * bdb_state->children[i]->version_num);*/
            }
        }
    }

    /* Recheck and copy back */
    bdb_lock_children_lock(bdb_state);
    if (bdb_state->numchildren != tablecount)
        retry = 1;
    for (int i = 0; i < tablecount && retry == 0; i++) {
        if ((tablenames[i] && !bdb_state->children[i]) ||
            (!tablenames[i] && bdb_state->children[i]) ||
            (tablenames[i] && bdb_state->children[i] &&
             strcmp(tablenames[i], bdb_state->children[i]->name))) {
            retry = 1;
            /* Update children version number if it hasn't been set (is 0) */
        } else if (bdb_state->children[i] &&
                   bdb_state->children[i]->version_num > 0 &&
                   bdb_state->children[i]->version_num !=
                       tran->table_version_cache[i]) {
            retry = 1;
        }
    }
    if (!retry) {
        for (int i = 0; i < tablecount; i++) {
            if (bdb_state->children[i])
                bdb_state->children[i]->version_num =
                    tran->table_version_cache[i];
        }
    }
    bdb_unlock_children_lock(bdb_state);

done:
    if (tablenames) {
        for (int i = 0; i < tablecount; i++) {
            if (tablenames[i])
                free(tablenames[i]);
        }
        free(tablenames);
    }
    if (retry != 0)
        goto retry;

    return rc;
}

int bdb_osql_check_table_version(bdb_state_type *bdb_state, tran_type *tran,
                                 int trak, int *bdberr)
{
    int i = 0;
    bdb_state_type *parent;

    parent = bdb_state->parent;
    assert(parent != 0);

    bdb_lock_children_lock(bdb_state);
    for (i = 0; i < parent->numchildren; i++) {
        if (bdb_state == parent->children[i]) {
            break;
        }
    }
    if (i == parent->numchildren) /* this looks more like a locking bug */
        i = -1;

    bdb_unlock_children_lock(bdb_state);

    if ((i >= 0) && (i < tran->table_version_cache_sz) &&
        (tran->table_version_cache[i] != 0) &&
        (tran->table_version_cache[i] == bdb_state->version_num)) {
        /*printf("OK %s [%d] %llx vs %llx\n", bdb_state->name, i,
         * tran->table_version_cache[i], bdb_state->version_num);*/
        return 0;
    } else {
        logmsg(LOGMSG_ERROR, "FAILED table \"%s\" changed, index=%d\n", bdb_state->name, i);
    }

    return -1;
}

int getpgsize(void *handle_)
{
    bdb_state_type *handle = handle_;
    DB *dbp = handle->dbp_data[0][0];
    int x = 0;
    dbp->get_pagesize(dbp, (u_int32_t *)&x);
    return x;
}

void bdb_set_key_compression(bdb_state_type *bdb_state)
{
    if (!gbl_keycompr)
        return;
    int i;
    DB *db;
    uint8_t flags;
    // COMPRESS KEY IN DTA FILES
    if (bdb_state->lrl < bdb_state->attr->genid_comp_threshold ||
        (bdb_state->compress &&
         bdb_state->lrl < (bdb_state->attr->genid_comp_threshold * 2))) {
        flags = DB_PFX_COMP;
        if (bdb_state->inplace_updates)
            flags |= DB_SFX_COMP;
        for (i = 0; i < bdb_state->attr->dtastripe; ++i) {
            db = bdb_state->dbp_data[0][i];
            db->set_compression_flags(db, flags);
        }
    }
    // COMPRESS KEY IN IX FILES
    flags = DB_PFX_COMP | DB_RLE_COMP;
    for (i = 0; i < bdb_state->numix; ++i) {
        db = bdb_state->dbp_ix[i];
        db->set_compression_flags(db, flags);
    }
}

#define YESNO(x) ((x) ? "yes" : "no")
void bdb_print_compression_flags(bdb_state_type *bdb_state)
{
    DB *db = bdb_state->dbp_data[0][0];
    uint8_t flags = db->get_compression_flags(db);
    logmsg(LOGMSG_USER, "table:%s data-> pfx:%s sfx:%s rle:%s", bdb_state->name,
           YESNO(flags & DB_PFX_COMP), YESNO(flags & DB_SFX_COMP),
           YESNO(flags & DB_RLE_COMP));
    if (bdb_state->numix <= 0) {
        logmsg(LOGMSG_ERROR, "\n");
        return;
    }
    db = bdb_state->dbp_ix[0];
    flags = db->get_compression_flags(db);
    logmsg(LOGMSG_ERROR, "   keys-> pfx:%s sfx:%s rle:%s\n", YESNO(flags & DB_PFX_COMP),
           YESNO(flags & DB_SFX_COMP), YESNO(flags & DB_RLE_COMP));
}

int bdb_enable_page_scan_for_table(bdb_state_type *bdb_state)
{
    assert(bdb_state->parent);
    bdb_state->disable_page_order_tablescan = 0;
    return 0;
}

int bdb_disable_page_scan_for_table(bdb_state_type *bdb_state)
{
    assert(bdb_state->parent);
    bdb_state->disable_page_order_tablescan = 1;
    return 0;
}

int bdb_get_page_scan_for_table(bdb_state_type *bdb_state)
{
    assert(bdb_state->parent);
    return !(bdb_state->disable_page_order_tablescan);
}

#define ERRDONE                                                                \
    do {                                                                       \
        rc = -1;                                                               \
        goto done;                                                             \
    } while (0)
/* Basic IO test.  Open a file, write a page, read a page, close file, delete
 * it. */
static int bdb_watchdog_test_io_dir(bdb_state_type *bdb_state, char *dir)
{
    int fd = -1;
    char *path = NULL;
    int pathlen;
    void *buf = NULL;
    int use_directio = bdb_attr_get(bdb_state->attr, BDB_ATTR_DIRECTIO);
    int flags = 0;
    int rc = 0;
    const int bufsz = 4096;
    const int align = 4096;
    const char wdog[] = "watchdog";

    /* We can supposedly allocate memory - that check is done before this one.
     * If memory allocation broke between then and now, we'll flag a wrong
     * failure.
     * But it'll trip the watchdog timer anyway. */
    pathlen = strlen(dir) + sizeof(wdog) + 1;
    path = malloc(pathlen);
    if (path == NULL) {
        logmsg(LOGMSG_ERROR, "Can't allocate filename buffer\n");
        ERRDONE;
    }
    sprintf(path, "%s/%s", dir, wdog);

    rc = posix_memalign(&buf, align, bufsz);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Can't allocate page buffer\n");
        ERRDONE;
    }
    memset(buf, 0, bufsz);

    flags = O_CREAT | O_TRUNC | O_RDWR;
#if !defined(_SUN_SOURCE) && !defined(__APPLE__)
    if (use_directio)
        flags |= O_DIRECT;
#endif
    fd = open(path, flags, 0666);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR, "Can't open/create %s: %d %s\n", path, errno,
                strerror(errno));
        ERRDONE;
    }
#ifdef _SUN_SOURCE
    if (use_directio)
        directio(fd, DIRECTIO_ON);
#elif defined(__APPLE__)
    if (use_directio)
        fcntl(fd, F_SETFL, F_NOCACHE);
#endif

    /* Can I write? */
    rc = pwrite(fd, buf, bufsz, 0);
    if (rc != bufsz) {
        logmsg(LOGMSG_ERROR, "write %s rc %d errno %d %s\n", path, rc, errno,
               strerror(errno));
        ERRDONE;
    }
    /* If not directio, flush - we are trying to test IO, but filesystem
     * buffering. */
    if (!use_directio) {
        rc = fsync(fd);
        if (rc) {
            logmsg(LOGMSG_ERROR, "sync %s rc errno %d %s\n", path, errno,
                   strerror(errno));
            ERRDONE;
        }
    }

    /* Can I read? */
    rc = pread(fd, buf, bufsz, 0);
    if (rc != bufsz) {
        logmsg(LOGMSG_ERROR, "read %s rc %d errno %d %s\n", path, rc, errno,
               strerror(errno));
        ERRDONE;
    }

    /* If we get this far, let's call basic IO working */
    rc = 0;

done:
    // printf("watchdog %s fd %d test rc %d\n", path, fd, rc);
    if (path) {
        /* If we can't clean up, just warn. */
        if (unlink(path))
            logmsg(LOGMSG_ERROR, "unlink(%s) rc %d %s\n", path, errno,
                    strerror(errno));
        free(path);
    }
    if (fd != -1)
        close(fd);
    os_free(buf);

    return rc;
}
#undef ERRDONE

/* Test writing to the directories we care about. */
int bdb_watchdog_test_io(bdb_state_type *bdb_state)
{
    return bdb_watchdog_test_io_dir(bdb_state, bdb_state->dir) ||
           bdb_watchdog_test_io_dir(bdb_state, bdb_state->txndir) ||
           bdb_watchdog_test_io_dir(bdb_state, bdb_state->tmpdir);
    /* TODO: Should we test writing to log directory?  That may
     * legitimately be not writable, but blocking forever writing to it can
     * still
     * break the database. */
}

void rename_bdb_state(bdb_state_type *bdb_state, const char *newname)
{
    if (bdb_state->name)
        free(bdb_state->name);
    bdb_state->name = strdup(newname);
}

int bdb_list_all_fileids_for_newsi(bdb_state_type *bdb_state,
                                   hash_t *fileid_tbl)
{
    const char blob_ext[] = ".blob";
    const char data_ext[] = ".data";
    const char index_ext[] = ".index";

    DB_ENV *dbenv;
    DB *dbp;

    struct dirent *buf;
    struct dirent *ent;
    DIR *dirp;
    int error;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    buf = malloc(bb_dirent_size(bdb_state->dir));
    if (!buf) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }

    dbenv = bdb_state->dbenv;

    dirp = opendir(bdb_state->dir);
    if (!dirp) {
        logmsg(LOGMSG_ERROR, "%s: opendir failed\n", __func__);
        free(buf);
        return -1;
    }

    while ((error = bb_readdir(dirp, buf, &ent)) == 0 && ent != NULL) {
        if (strlen(ent->d_name) > 5 &&
            (strstr(ent->d_name, blob_ext) || strstr(ent->d_name, data_ext) ||
             strstr(ent->d_name, index_ext))) {
            char munged_name[FILENAMELEN];
            char transname[256];
            char *pname = NULL;
            unsigned char *fileid = NULL;
            if (snprintf(munged_name, sizeof(munged_name), "XXX.%s",
                         ent->d_name) >= sizeof(munged_name)) {
                logmsg(LOGMSG_ERROR, "%s: filename too long to munge: %s\n",
                       __func__, ent->d_name);
                closedir(dirp);
                free(buf);
                return -1;
            }
            pname = bdb_trans(munged_name, transname);

            if (db_create(&dbp, dbenv, 0) == 0 &&
                dbp->open(dbp, NULL, pname, NULL, DB_BTREE, 0, 0666) == 0) {
                fileid = malloc(DB_FILE_ID_LEN);
                if (fileid == NULL) {
                    closedir(dirp);
                    free(buf);
                    return -1;
                }
                memcpy(fileid, dbp->fileid, DB_FILE_ID_LEN);
                hash_add(fileid_tbl, fileid);
#ifdef NEWSI_DEBUG
                char *txt;
                hexdumpbuf(fileid, DB_FILE_ID_LEN, &txt);
                logmsg(LOGMSG_DEBUG, "%s: hash_add fileid %s\n", __func__, txt);
                free(txt);
#endif
                dbp->close(dbp, DB_NOSYNC);
            }
        }
    }

    closedir(dirp);
    free(buf);
    return 0;
}

void bdb_assert_notran(bdb_state_type *bdb_state)
{
    bdb_state->dbenv->txn_assert_notran(bdb_state->dbenv);
}

int bdb_debug_log(bdb_state_type *bdb_state, tran_type *trans, int inop)
{
    DB_TXN *tid = trans ? trans->tid : NULL;
    int endianized = htonl(inop);
    DBT op = {0};
    op.size = sizeof(int);
    op.data = &endianized;
    return bdb_state->dbenv->debug_log(bdb_state->dbenv, tid, &op, NULL, NULL);
}
