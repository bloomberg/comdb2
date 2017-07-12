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
 * bdb layer public api
 *
 * $Id$
 */

#ifndef __bdb_api_h__
#define __bdb_api_h__

#include <stdio.h>
#include <stdarg.h>
#include <stdbool.h>

#include <sbuf2.h>

#include <net.h>
#include <bb_stdint.h>

#include <inttypes.h>
#include <limits.h>

#include <assert.h>
/*#include "protobuf/sqlresponse.pb-c.h"*/

#define SIZEOF_SEQNUM (10 * sizeof(int))
struct seqnum_t;
typedef struct seqnum_t seqnum_type;

struct filepage_t;
typedef struct filepage_t filepage_type;

struct bdb_state_tag;
typedef struct bdb_state_tag bdb_state_type;

struct bdb_callback_tag;
typedef struct bdb_callback_tag bdb_callback_type;

struct tran_tag;
typedef struct tran_tag tran_type;

struct bdb_attr_tag;
typedef struct bdb_attr_tag bdb_attr_type;

struct bdb_temp_hash;
typedef struct bdb_temp_hash bdb_temp_hash;

struct bulk_dump;
typedef struct bulk_dump bulk_dump;

struct dtadump;
typedef struct dtadump dtadump;

typedef struct bdb_cursor_ser bdb_cursor_ser_t;
struct bdb_cursor_ser {
    uint8_t opaque[64];
};

void bdb_cursor_ser_invalidate(bdb_cursor_ser_t *cur_ser);

enum {
    BDB_CALLBACK_NODEUP,
    BDB_CALLBACK_WHOISMASTER,
    BDB_CALLBACK_REPFAIL,
    BDB_CALLBACK_APPSOCK,
    BDB_CALLBACK_PRINT,
    BDB_CALLBACK_ELECTSETTINGS,
    BDB_CALLBACK_GETROOM,
    BDB_CALLBACK_CATCHUP,
    BDB_CALLBACK_THREADDUMP,
    BDB_CALLBACK_SENDNOTCOHERENT,
    BDB_CALLBACK_GETLWM,
    BDB_CALLBACK_SETLWM,
    BDB_CALLBACK_SCDONE,
    BDB_CALLBACK_SCABORT,
    BDB_CALLBACK_UNDOSHADOW,
    BDB_CALLBACK_NODE_IS_DOWN,
    BDB_CALLBACK_SERIALCHECK
};

enum { BDB_REPFAIL_NET, BDB_REPFAIL_TIMEOUT, BDB_REPFAIL_RMTBDB };

enum { BDB_OP_ADD = 1, BDB_OP_DEL = 2 };

/* debug options */
enum {
    SQL_DBG_NONE = 0, /* no debug, default */
    SQL_DBG_BDBLOG =
        1, /* track the osqltrn bdblog operations (osqltrn has the flag) */
    SQL_DBG_BDBALLLOG = 2, /* track all bdblog operations (no osqltrn here,
                              osqllog_repo has the flag, pushed to osqllog) */
    SQL_DBG_BDBTRN = 4,    /* track the osqltrn transaction registration
                              operations (osqltrn has the flag) */
    SQL_DBG_BDBALLTRN = 8, /* track the osqltrn registration operations
                              (osqltrn repo has the flag, trak all osqltrn) */
    SQL_DBG_SHADOW = 16,   /* track inserts to shadow files */
    SQL_DBG_ALL = INT_MAX  /* enable all debug options */
};

typedef struct {
    int opcode;
    int ixnum;
    void *ixdta;
} bdb_update_op_type;

/* Queue stats */
struct bdb_queue_stats {
    unsigned n_new_way_frags_consumed;
    unsigned n_new_way_frags_aborted;
    unsigned n_new_way_geese_consumed;
    unsigned n_old_way_frags_consumed;

    unsigned n_consume_deadlocks;
    unsigned n_add_deadlocks;
    unsigned n_get_deadlocks;
    unsigned n_get_not_founds;

    unsigned n_logical_gets;
    unsigned n_physical_gets;
};

/* This is identical to bb_berkdb_thread_stats in db.h */

/*
 * Multiplication usually takes fewer CPU cycles than division. Therefore
 * when comparing a usec and a msec, it is preferable to use:
 * usec <comparison operator> M2U(msec)
 */
#ifndef U2M
#define U2M(usec) (int)((usec) / 1000)
#endif
#ifndef M2U
#define M2U(msec) ((msec)*1000ULL)
#endif
struct bdb_thread_stats {
    unsigned n_lock_waits;
    uint64_t lock_wait_time_us;

    unsigned n_preads;
    unsigned pread_bytes;
    uint64_t pread_time_us;

    unsigned n_pwrites;
    unsigned pwrite_bytes;
    uint64_t pwrite_time_us;

    unsigned n_memp_fgets;
    uint64_t memp_fget_time_us;

    unsigned n_memp_pgs;
    uint64_t memp_pg_time_us;

    unsigned n_shallocs;
    uint64_t shalloc_time_us;

    unsigned n_shalloc_frees;
    uint64_t shalloc_free_time_us;
};

/* these are the values that "bdberr" can be */
enum {
    BDBERR_NOERROR = 0,
    BDBERR_MISC = 2, /* no 1 error code, precious? */
    BDBERR_MANIPULATE_FREEREC = 3,
    BDBERR_ADD_DTA = 4,
    BDBERR_ADD_IX = 5,
    BDBERR_ADD_RRN = 6,
    BDBERR_DEADLOCK = 7,
    BDBERR_BUFSMALL = 8,
    BDBERR_ADD_DUPE = 9,
    BDBERR_DEL_DTA = 10,
    BDBERR_DEL_IX = 11,
    BDBERR_DEL_RRN = 12,
    BDBERR_DELNOTFOUND = 13,
    BDBERR_BADARGS = 14,
    BDBERR_FETCH_DTA = 15,
    BDBERR_FETCH_IX = 16,
    BDBERR_RRN_NOTFOUND = 17,
    BDBERR_DTA_MISMATCH = 18,
    BDBERR_DBEMPTY = 19,
    BDBERR_RESERVED_1 = 20, /* internal use only */
    BDBERR_READONLY = 21,
    BDBERR_TRANTOOCOMPLEX = 22,
    BDBERR_CALLBACK = 23,
    BDBERR_MALLOC = 24,
    BDBERR_IO = 25,      /* io error e.g. in fastdump */
    BDBERR_TIMEOUT = 26, /* e.g. fast dump socket timeout */
    BDBERR_UNPACK = 27,  /* unable to unpack ODH */
    BDBERR_PACK = 28,    /* unable to pack ODH */
    BDBERR_INCOHERENT = 29,
    BDBERR_BUG_KILLME = 30,
    BDBERR_ROW_DEADLOCK = 31,
    BDBERR_INVALID_LSN = 32,
    BDBERR_DEADLOCK_ON_LAST = 33,
    BDBERR_TRAN_CANCELLED = 34,
    BDBERR_NO_LOG = 35,
    BDBERR_DEADLOCK_ROWLOCK = 36,
    BDBERR_NEED_REPOSITION = 37,
    BDBERR_LOCK_DESIRED = 38,
    BDBERR_NOT_DURABLE = 39
};

/* values for BDB_ATTR_LOGDELETEAGE; +ve values indicate an absolute
 * unix epoch time. */
enum { LOGDELETEAGE_NEVER = -1, LOGDELETEAGE_NOW = 0 };

enum {
    BDB_ATTRTYPE_SECS,
    BDB_ATTRTYPE_MSECS,
    BDB_ATTRTYPE_USECS,
    BDB_ATTRTYPE_BYTES,
    BDB_ATTRTYPE_KBYTES,
    BDB_ATTRTYPE_MBYTES,
    BDB_ATTRTYPE_BOOLEAN,
    BDB_ATTRTYPE_QUANTITY,
    BDB_ATTRTYPE_PERCENT
};

/* See attr.h for attribute definitions */
enum {
#define DEF_ATTR(NAME, name, type, dflt, desc) BDB_ATTR_##NAME,
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    BDB_ATTR_##NAME,
#include "attr.h"
#undef DEF_ATTR
#undef DEF_ATTR_2
    BDB_ATTR_MAX
};

/*
 * Backend thread event constants.
 */
enum {
    BDBTHR_EVENT_DONE_RDONLY = 0,
    BDBTHR_EVENT_START_RDONLY = 1,
    BDBTHR_EVENT_DONE_RDWR = 2,
    BDBTHR_EVENT_START_RDWR = 3
};

/*
 * table types supported by bdblib
 */
typedef enum {
    BDBTYPE_NONE = 0,
    BDBTYPE_ENV = 1,     /* environment */
    BDBTYPE_TABLE = 2,   /* normal table */
    BDBTYPE_LITE = 3,    /* single .dta file, one index, no .ix files */
    BDBTYPE_QUEUE = 4,   /* mainly in bdbqueue.c */
    BDBTYPE_QUEUEDB = 5, /* like BDBTYPE_QUEUE, but use btree */
} bdbtype_t;

enum {
    BDB_KEY_MAX = 512,     /*
                            max size of a key ON DISK.
                            comdb2 exposes 256 bytes, with 256 columns and
                            1 byte of overhead per, that gets us to 512
                          */
    BDB_RECORD_MAX = 20480 /*
                             max size of a fixed record ON DISK.
                             comdb2 exposes 16384.  add 1 byte for an
                             amazing 4k of columns (we dont support that)
                             and get to 20480.

                             note, this would be HORRIBLE
                             as the fixed max page size in berk is 65536
                             and they need to be able to fit 4 records on
                             a page (or else use overflow) including some
                             berk overhead.  so that means the max bdb
                             record size should realistically be some number
                             like 15384 if we want to perform.  oh well.

                             even better, with 4k pages, we shouldnt be
                             using record sizes more than 900k or so.
                            */
};

enum COMPRESS {
    BDB_COMPRESS_NONE = 0,
    BDB_COMPRESS_ZLIB = 1,
    BDB_COMPRESS_RLE8 = 2,
    BDB_COMPRESS_CRLE = 3,
    BDB_COMPRESS_LZ4 = 4
};

int bdb_compr2algo(const char *a);
const char *bdb_algo2compr(int a);

/* retrieve the user pointer associated with a bdb_handle */
void *bdb_get_usr_ptr(bdb_state_type *bdb_handle);

/* CALLBACK ROUTINES */

/*
  provide a "printf()" compatible call for bdb trace to be output with.
  in the absense of this callback, all bdb trace will go to stderr.
  */
typedef int (*PRINTFP)(const char *format, va_list ap);

/*
  provide a routine that returns 0 or 1 when given the id of a node.
  return 0 if the node is marked "down"
  return 1 if the node is marked "up"
  this routine will be used to determine if nodes should be pulled
  from the election pool when errors are occuring talking to this
  node.
*/
typedef int (*NODEUPFP)(bdb_state_type *bdb_handle, const char *host);

/*
   provide a callback that gets called when a node disconnects.
   this is needed by higher levels that need to stop waiting
   for that node.
*/
typedef int (*NODEDOWNFP)(char *host);

/*
   provide a callback that gets called doing serializable
   transaction read-set validation.
   This is needed by higher levels that need to abort non-serializable
   transaction.
*/
typedef int (*SERIALCHECK)(char *tbname, int idxnum, void *key, int keylen,
                           void *ranges);

/*
  provide a routine that returns an integer specifying the "room"
  of a given node.  this is used to keep coherency within a room.
*/
typedef int (*GETROOMFP)(bdb_state_type *bdb_handle, const char *host);

/*
  pass in a routine that will be called to tell you that someone
  has become the master.  take whatever action necessary to get
  updates directed to you if you are now the master or to have you
  updates directed elesewhere if you learned of a new master.
  do NOT call back into the bdb library from this routine.
*/
typedef int (*WHOISMASTERFP)(bdb_state_type *bdb_handle, char *host);

/*
  pass in a routine that will be called when the replication
  subsystem has an error communicating with a node.  reason
  for failure will be passed into this routine.  reasons can
  be BDB_REPFAIL_NET, BDB_REPFAIL_TIMEOUT, BDB_REPFAIL_RMTBDB
  BDB_REPFAIL_NET indicates a network level error communicating
  with a node.  BDB_REPFAIL_TIMEOUT indicates a timeout (as specified
  by BDB_ATTR_REPTIMEOUT) communicating with a node.
  BDB_REPFAIL_RMTBDB indicates that the bdb library on the remote
  node returned failure.
  if user requests are still being directed to a node that is
  generating repfail events, the coherency of the data on that node
  is at risk with being out of date with respect the the actual data
  as it exists on the master copy of the database.
*/
typedef int (*REPFAILFP)(bdb_state_type *bdb_handle, char *host, int reason);

/*
  pass in a routine that will handle a newly created socket.
  this routine must return immediately, and should create it's own
  thread if extended processing is needed.
  */
typedef int (*BDBAPPSOCKFP)(bdb_state_type *bdb_handle, SBUF2 *sb);

/*
  pass in a routine that will return the current election preferences.
  for now the only thing it can change is the election timeout value,
  which is specified in seconds.  it should alays return 0.
  */
typedef int (*BDBELECTSETTINGSFP)(bdb_state_type *bdb_handle,
                                  int *elect_time_secs);

/*
  pass in a routine that will be called when election has succeeded
  and the database starts catching up to the master
*/
typedef int (*BDBCATCHUPFP)(bdb_state_type *bdb_handle,
                            unsigned long long ourlsn,
                            unsigned long long masterlsn);

typedef void (*BDBTHREADDUMPFP)(void);

typedef void (*BDBSENDNOTCOHERENTFP)(char *node, int notcoherent, int file,
                                     int offset, int *rc);

typedef int (*BDBGETFILELWMFP)(int *);
typedef int (*BDBSETFILELWMFP)(int *);
/* retrieve all snapshot/serializable sql sessions and update their shadow
   tables to account for committed deletes and hide adds */
struct bdb_osql_log;
typedef void (*UNDOSHADOWFP)(struct bdb_osql_log *);

typedef int (*BDB_CALLBACK_FP)();
bdb_callback_type *bdb_callback_create(void);
void bdb_callback_set(bdb_callback_type *bdb_callback, int callback_type,
                      BDB_CALLBACK_FP callback_rtn);

/*
  create a bdb attribute object.  all attributes always have default
  varaibles.  defaults are documented in the BDB_ATTR enum.
*/
void *bdb_attr_create(void);

/*
  set an attribute in a bdb attribute object to the specfied value.
*/
void bdb_attr_set(bdb_attr_type *bdb_attr, int attr, int value);
int bdb_attr_set_by_name(bdb_state_type *bdb_handle, bdb_attr_type *bdb_attr,
                         const char *attrname, int value);

int bdb_attr_get(bdb_attr_type *bdb_attr, int attr);
#define BDB_ATTR_GET(bdb_attr, attr) (bdb_attr_get(bdb_attr, BDB_ATTR_##attr))
void bdb_attr_dump(FILE *fh, const bdb_attr_type *bdb_attr);

/* Get the type of this bdb-state object as a BDBTYPE_ constant */
bdbtype_t bdb_get_type(bdb_state_type *bdb_state);

bdb_state_type *bdb_clone_handle_with_other_data_files(
    const bdb_state_type *clone_bdb_state,
    const bdb_state_type *data_files_bdb_state);
void bdb_free_cloned_handle_with_other_data_files(bdb_state_type *bdb_state);

/*
  bdb_open_more() : "open" a new database. associate this db transactionally
                     with the bdb_handle from a prior bdb_open() call.
                     the returned bdb_handle will be associated with the
                     passed in bdb_handle.  cache will be shared.
                     calls to bdb_transaction_begin()/commit()/abort() on
                     any associated bdb_handle operate on all associated
                     bdb_handles as a unit.

  INPUT:             name         : "comdb name"
                     dir          : location of all files (/bb/data or
                                    something)
                     numix        : number of indexes on database
                     ixlen        : numix long array of integers specifying
                                    size of each index in bytes
                     ixdups       : array of ints, 1 == ix allows dups,
                                    0 == ix does not allow dups
                     btree_recnum : 1 == turn on recnums on btrees.
                                    0 == don't turn on recnums on btrees.
                     bdb_handle   : a valid bdb_handle obtained from a
                                    bdb_open() call.

  OUTPUT:            RETURN : success : pointer to bdb handle
                              failure : NULL
                     bdberr : will be set to provide additional
                              info on failure.  possibilities are:
                      catastrophic:
                       BDBERR_MISC : some problem occurred
*/

/* open an existing table */
bdb_state_type *
bdb_open_more(const char name[], const char dir[], int lrl, short numix,
              const short ixlen[], const signed char ixdups[],
              const signed char ixrecnum[], const signed char ixdta[],
              const signed char ixcollattr[], const signed char ixnulls[],
              int numdtafiles, bdb_state_type *parent_bdb_handle, int *bdberr);

/* same, but using a transaction */
bdb_state_type *
bdb_open_more_tran(const char name[], const char dir[], int lrl, short numix,
                   const short ixlen[], const signed char ixdups[],
                   const signed char ixrecnum[], const signed char ixdta[],
                   const signed char ixcollattr[], const signed char ixnulls[],
                   int numdtafiles, bdb_state_type *parent_bdb_handle,
                   tran_type *tran, int *bdberr);

/* open an existing lite table */
bdb_state_type *bdb_open_more_lite(const char name[], const char dir[], int lrl,
                                   int ixlen, int pagesize,
                                   bdb_state_type *parent_bdb_handle,
                                   int *bdberr);

/* open an existing queue */
bdb_state_type *bdb_open_more_queue(const char name[], const char dir[],
                                    int item_size, int pagesize,
                                    bdb_state_type *parent_bdb_state,
                                    int isqueuedb, int *bdberr);

/* create a new queue */
bdb_state_type *bdb_create_queue(const char name[], const char dir[],
                                 int item_size, int pagesize,
                                 bdb_state_type *parent_bdb_state,
                                 int isqueuedb, int *bdberr);

/* create a lite table */
bdb_state_type *bdb_create_more_lite(const char name[], const char dir[],
                                     int lrl, int ixlen, int pagesize,
                                     bdb_state_type *parent_bdb_handle,
                                     int *bdberr);

/* create and open a new table */
bdb_state_type *
bdb_create(const char name[], const char dir[], int lrl, short numix,
           const short ixlen[], const signed char ixdups[],
           const signed char ixrecnum[], const signed char ixdta[],
           const signed char ixcollattr[], const signed char ixnulls[],
           int numdtafiles, bdb_state_type *parent_bdb_handle, int temp,
           int *bdberr);

bdb_state_type *
bdb_create_tran(const char name[], const char dir[], int lrl, short numix,
                const short ixlen[], const signed char ixdups[],
                const signed char ixrecnum[], const signed char ixdta[],
                const signed char ixcollattr[], const signed char ixnulls[],
                int numdtafiles, bdb_state_type *parent_bdb_handle, int temp,
                int *bdberr, tran_type *);

/* open a databasent.  no actual db files are created. */
bdb_state_type *bdb_open_env(const char name[], const char dir[],
                             bdb_attr_type *bdb_attr,
                             bdb_callback_type *bdb_callback, void *usr_ptr,
                             netinfo_type *netinfo,
                             netinfo_type *netinfo_signal, char *recoverlsn,
                             int *bdberr);

int bdb_set_all_contexts(bdb_state_type *bdb_state, int *bdberr);
int bdb_handle_reset(bdb_state_type *);
int bdb_handle_reset_tran(bdb_state_type *, tran_type *);
int bdb_handle_dbp_add_hash(bdb_state_type *bdb_state, int szkb);
int bdb_handle_dbp_drop_hash(bdb_state_type *bdb_state);
int bdb_handle_dbp_hash_stat(bdb_state_type *bdb_state);
int bdb_handle_dbp_hash_stat_reset(bdb_state_type *bdb_state);
int bdb_close_temp_state(bdb_state_type *bdb_state, int *bdberr);

/* get file sizes for indexes and data files */
uint64_t bdb_index_size(bdb_state_type *bdb_state, int ixnum);
uint64_t bdb_data_size(bdb_state_type *bdb_state, int dtanum);
uint64_t bdb_queue_size(bdb_state_type *bdb_state, unsigned *num_extents);
uint64_t bdb_logs_size(bdb_state_type *bdb_state, unsigned *num_logs);

/*
  bdb_close(): destroy a bdb_handle.
*/
int bdb_close(bdb_state_type *bdb_handle);

/* see if a handle is open or not */
int bdb_isopen(bdb_state_type *bdb_handle);

/* you need to call this if you created the parent with bdb_open_env */
int bdb_close_env(bdb_state_type *bdb_handle);

/* get a context that can be used in fetches to ensure that we don't fetch
 * records added after the context of the original find. */
unsigned long long bdb_get_cmp_context(bdb_state_type *bdb_state);

unsigned long long bdb_get_cmp_context_local(bdb_state_type *bdb_state);

/* Check that the given genid is older than the compare context being given.
 * Returns: 1 genid is older, 0 genid is newer */
int bdb_check_genid_is_older(bdb_state_type *bdb_state,
                             unsigned long long genid,
                             unsigned long long context);

/* Compare two genids to determine which one would have been allocated first.
 * Return codes:
 *    -1    a < b
 *    0     a == b
 *    1     a > b
 */
int bdb_cmp_genids(unsigned long long a, unsigned long long b);

/* Mask-out the inplace update-id for each genid and then compare them to
 * see which one would have been allocated first:
 * Return codes:
 *    -1    a < b
 *    0     a == b
 *    1     a > b
 */
int bdb_inplace_cmp_genids(bdb_state_type *bdb_state, unsigned long long g1,
                           unsigned long long g2);

/* Retrieve the participant stripe id which is encoded in the genid.
 * Return codes:
 *    -1    there are no bits allocated for participant stripe id
 *    otherwise, the stripe-id associated with the genid
 */
int bdb_get_participant_stripe_from_genid(bdb_state_type *bdb_state,
                                          unsigned long long genid);

/* Mask a genid so that it can be used in an ondisk file.  With ODH
 * turned on this masks out the updateid field.  We use this in live schema
 * change since some old databases may have genids that have values in this
 * field, so we have to change those genids when we do this conversion. */
unsigned long long bdb_mask_updateid(bdb_state_type *bdb_state,
                                     unsigned long long genid);

/* Normalize a genid so that we can find and delete it during a live
 * schema-change.  This is necessary if schema-change is removing ondisk
 * headers for a database table which had in-place updates enabled.  It's
 * used to locate and delete a record in the new, odh-less table. */
unsigned long long bdb_normalise_genid(bdb_state_type *bdb_state,
                                       unsigned long long genid);

/* return a new tran handle, begin a transaction */
tran_type *bdb_tran_begin(bdb_state_type *bdb_handle, tran_type *parent_tran,
                          int *bdberr);

tran_type *bdb_tran_begin_mvcc(bdb_state_type *bdb_handle,
                               tran_type *parent_tran, int *bdberr);

tran_type *bdb_tran_begin_stable(bdb_state_type *bdb_handle,
                                 tran_type *parent_tran, int *bdberr);

tran_type *bdb_tran_begin_dirty(bdb_state_type *bdb_handle,
                                tran_type *parent_tran, int *bdberr);

tran_type *bdb_tran_begin_logical(bdb_state_type *bdb_state, int trak,
                                  int *bdberr);

tran_type *bdb_start_ltran(bdb_state_type *bdb_state,
                           unsigned long long ltranid, void *firstlsn,
                           unsigned int flags);
tran_type *bdb_start_ltran_rep_sc(bdb_state_type *bdb_state,
                                  unsigned long long ltranid);
void bdb_set_tran_lockerid(tran_type *tran, uint32_t lockerid);
void bdb_get_tran_lockerid(tran_type *tran, uint32_t *lockerid);
void *bdb_get_physical_tran(tran_type *ltran);
void bdb_ltran_get_schema_lock(tran_type *ltran);

tran_type *bdb_tran_begin_socksql(bdb_state_type *, int trak, int *bdberr);

tran_type *bdb_tran_begin_readcommitted(bdb_state_type *, int trak,
                                        int *bdberr);

tran_type *bdb_tran_begin_serializable(bdb_state_type *bdb_state, int trak,
                                       int *bdberr, int epoch, int file,
                                       int offset);
tran_type *bdb_tran_begin_snapisol(bdb_state_type *bdb_state, int trak,
                                   int *bdberr, int epoch, int file,
                                   int offset);

/* commit the transaction referenced by the tran handle */
int bdb_tran_commit(bdb_state_type *bdb_handle, tran_type *tran, int *bdberr);

int bdb_tran_commit_logical_with_seqnum_size(bdb_state_type *bdb_state,
                                             tran_type *tran, void *blkseq,
                                             int blklen, void *blkkey,
                                             int blkkeylen, seqnum_type *seqnum,
                                             uint64_t *out_txnsize,
                                             int *bdberr);

int bdb_tran_get_start_file_offset(bdb_state_type *bdb_state, tran_type *tran, int *file, int *offset);

/* commit the transaction referenced by the tran handle.  return a
   seqnum that is guaranteed to be greater or equal to the seqnum
   needed to have this commit reflected in your database */
int bdb_tran_commit_with_seqnum(bdb_state_type *bdb_state, tran_type *tran,
                                seqnum_type *seqnum, int *bdberr);

/* same, but also return an estimate of the transaction size in unspecified
 * units */
int bdb_tran_commit_with_seqnum_size(bdb_state_type *bdb_state, tran_type *tran,
                                     seqnum_type *seqnum, uint64_t *out_txnsize,
                                     int *bdberr);

/* abort the transaction referenced by the tran handle */
int bdb_tran_abort(bdb_state_type *bdb_handle, tran_type *tran, int *bdberr);
int bdb_tran_abort_priority(bdb_state_type *bdb_handle, tran_type *tran,
                            int *bdberr, int *priority);

/* english doesn't have curses vile enough */
int bdb_tran_abort_logical(bdb_state_type *bdb_handle, tran_type *tran,
                           int *bdberr, void *blkseq, int blklen, void *blkkey,
                           int blkkeylen, seqnum_type *seqnum);

/* prim operations all require a valid tran to be held */
int bdb_prim_allocdta_genid(bdb_state_type *bdb_handle, tran_type *tran,
                            void *dtaptr, int dtalen, unsigned long long *genid,
                            int updateid, int *bdberr);
int bdb_prim_adddta_n_genid(bdb_state_type *bdb_state, tran_type *tran,
                            int dtanum, void *dtaptr, size_t dtalen, int rrn,
                            unsigned long long genid, int *bdberr);

int bdb_prim_deallocdta_genid(bdb_state_type *bdb_handle, tran_type *tran,
                              int rrn, unsigned long long genid, int *bdberr);
int bdb_prim_deallocdta_n_genid(bdb_state_type *bdb_state, tran_type *tran,
                                int rrn, unsigned long long genid, int dtanum,
                                int *bdberr);

int bdb_prim_updvrfy_genid(bdb_state_type *bdb_state, tran_type *tran,
                           void *olddta, int oldlen, void *newdta, int newdtaln,
                           int rrn, unsigned long long oldgenid,
                           unsigned long long *newgenid, int verifydta,
                           int participantstripeid, int use_new_genid,
                           int *bdberr);

int bdb_prim_add_upd_genid(bdb_state_type *bdb_state, tran_type *tran,
                           int dtanum, void *newdta, int newdtaln, int rrn,
                           unsigned long long oldgenid,
                           unsigned long long newgenid, int participantstripeid,
                           int *bdberr);

int bdb_prim_no_upd(bdb_state_type *bdb_state, tran_type *tran, int rrn,
                    unsigned long long oldgenid, unsigned long long newgenid,
                    int blobmap, int *bdberr);

int bdb_prim_updvrfy_genid(bdb_state_type *bdb_handle, tran_type *tran,
                           void *olddta, int oldlen, void *newdta, int newdtaln,
                           int rrn, unsigned long long oldgenid,
                           unsigned long long *newgenid, int verifydta,
                           int participantstripeid, int use_new_genid,
                           int *bdberr);
int bdb_upd_genid(bdb_state_type *bdb_state, tran_type *tran, int dtanum,
                  int rrn, unsigned long long oldgenid,
                  unsigned long long newgenid, int has_blob_opt, int *bdberr);

int bdb_prim_addkey_genid(bdb_state_type *bdb_handle, tran_type *tran,
                          void *ixdta, int ixnum, int rrn,
                          unsigned long long genid, void *dta, int dtalen,
                          int isnull, int *bdberr);
int bdb_prim_delkey_genid(bdb_state_type *bdb_handle, tran_type *tran,
                          void *ixdta, int ixnum, int rrn,
                          unsigned long long genid, int *bdberr);
int bdb_prim_updkey_genid(bdb_state_type *bdb_state, tran_type *tran, void *key,
                          int keylen, int ixnum, unsigned long long oldgenid,
                          unsigned long long genid, void *dta, int dtalen,
                          int *bdberr);

int bdb_prim_upgrade(bdb_state_type *bdb_state, tran_type *tran, void *newdta,
                     int newdtaln, unsigned long long oldgenid, int *bdberr);

/* Callbacks for use with range delete. */

/* Form the requested key for a given record.
 *
 * The arguments are:
 *  void *record            - the record to form a key for.  tag routines
 *                            tend to modify this in place, so not const
 *  size_t record_len       - length of the record data
 *  void *index             - buffer in which to form the index
 *  size_t index_len        - length of the index
 *  int index_num           - which index to form
 *  void *userptr           - user supplied data
 *
 * The callback implementation should return:
 *  0   - key formed successfully, go ahead and delete this record.
 *  -1  - halt the range delete operation with an error.
 *  -2  - do not delete this record, continue to look at other records.
 *  -3  - deadlock
 */
typedef int (*bdb_formkey_callback_t)(void *, size_t, void *, size_t, int,
                                      void *);

/* Called before a record is deleted; can be used to cancel the record
 * deletion.
 *
 * Arguments:
 *  void *record             - the record to form a key for.  tag routines
 *                             tend to modify this in place, so not const
 *  size_t record_len        - length of the record data
 *  int rrn                  - rrn number of record
 *  unsigned long long genid - genid of record
 *  void *userptr            - user supplied data
 *
 * The callback implementation should return:
 *  0   - go ahead and delete this record.
 *  -1  - halt the range delete operation with an error.
 *  -2  - do not delete this record, continue to look at other records.
 *  -3  - deadlock
 */
typedef int (*bdb_pre_delete_callback_t)(void *, size_t, int,
                                         unsigned long long, void *);

/* Called after a record has been deleted.
 *
 * Arguments:
 *  void *record             - the record to form a key for.  tag routines
 *                             tend to modify this in place, so not const
 *  size_t record_len        - length of the record data
 *  int rrn                  - rrn number of record
 *  unsigned long long genid - genid of record
 *  void *userptr            - user supplied data
 *
 * The callback implementation should return:
 *  0   - go ahead and delete this record.
 *  -1  - halt the range delete operation with an error.
 *  -3  - deadlock
 */
typedef int (*bdb_post_delete_callback_t)(void *, size_t, int,
                                          unsigned long long, void *);

/* For the best possible performance I've built range delete support directly
 * into bdblib.  It zooms through the given index and deletes all keys
 * between the start and end point given.  A callback function must be
 * provided to form keys from the given data record so that they can be
 * deleted.  The other callbacks are optional and can be NULL.
 *
 * Returns the number of records deleted, or -1 on error (check *bdberr)
 */
int bdb_prim_range_delete(bdb_state_type *bdb_handle, tran_type *tran,
                          size_t dtalen, int index, const void *start_key,
                          const void *end_key, size_t keylength,
                          bdb_formkey_callback_t formkey_callback,
                          bdb_pre_delete_callback_t pre_callback,
                          bdb_post_delete_callback_t post_callback,
                          void *userptr, int *count_deleted_ptr,
                          int *count_not_deleted_ptr, int max_records,
                          int max_time_ms, int *bdberr);

/* lite operations give you direct access to bdb tables with minimum overhead */
int bdb_lite_add(bdb_state_type *bdb_handle, tran_type *tran, void *dtaptr,
                 int dtalen, void *key, int *bdberr);
int bdb_lite_exact_del(bdb_state_type *bdb_handle, tran_type *tran, void *key,
                       int *bdberr);
int bdb_lite_exact_fetch(bdb_state_type *bdb_handle, void *key, void *fnddta,
                         int maxlen, int *fndlen, int *bdberr);
int bdb_lite_exact_fetch_alloc(bdb_state_type *bdb_handle, void *key,
                               void **fnddta, int *fndlen, int *bdberr);
int bdb_lite_exact_fetch_tran(bdb_state_type *bdb_state, tran_type *tran,
                              void *key, void *fnddta, int maxlen, int *fndlen,
                              int *bdberr);
int bdb_lite_exact_var_fetch(bdb_state_type *bdb_handle, void *key,
                             void **fnddta, int *fndlen, int *bdberr);
int bdb_lite_exact_var_fetch_tran(bdb_state_type *bdb_state, tran_type *tran,
                                  void *key, void **fnddta, int *fndlen,
                                  int *bdberr);
int bdb_lite_fetch_keys_fwd(bdb_state_type *bdb_state, void *firstkey,
                            void *fndkeys, int maxfnd, int *numfnd,
                            int *bdberr);
int bdb_lite_fetch_keys_fwd_tran(bdb_state_type *bdb_state, tran_type *tran,
                                 void *firstkey, void *fndkeys, int maxfnd,
                                 int *numfnd, int *bdberr);
int bdb_lite_fetch_keys_bwd(bdb_state_type *bdb_state, void *firstkey,
                            void *fndkeys, int maxfnd, int *numfnd,
                            int *bdberr);
int bdb_lite_fetch_keys_bwd_tran(bdb_state_type *bdb_state, tran_type *tran,
                                 void *firstkey, void *fndkeys, int maxfnd,
                                 int *numfnd, int *bdberr);
int bdb_lite_fetch_partial(bdb_state_type *bdb_state, void *key_in, int klen_in,
                           void *key_out, int *fnd, int *bdberr);

/* queue operations are for queue tables - fifos with multiple consumers */

enum { BDBQUEUE_MAX_CONSUMERS = 32 };

/* 16 byte pointer to an item in an ondisk queue. */
struct bdb_queue_cursor {
    bbuint32_t genid[2]; /* genid of item */
    bbuint32_t recno;    /* recno of first fragment of item */
    bbuint32_t reserved; /* must be zero */
};

/* mark a consumer as active or inactive.  this grabs the bdb write lock. */
int bdb_queue_consumer(bdb_state_type *bdb_state, int consumer, int active,
                       int *bdberr);

/* add an item to the end of the queue. */
int bdb_queue_add(bdb_state_type *bdb_state, tran_type *tran, const void *dta,
                  size_t dtalen, int *bdberr, unsigned long long *out_genid);

/* add/consume dummy records to aid extent reclaimation.  winner of the
 * May 2006 "Most Absurd Hack" award. */
int bdb_queue_check_goose(bdb_state_type *bdb_state, tran_type *tran,
                          int *bdberr);
int bdb_queue_add_goose(bdb_state_type *bdb_state, tran_type *tran,
                        int *bdberr);
int bdb_queue_consume_goose(bdb_state_type *bdb_state, tran_type *tran,
                            int *bdberr);

/* get the first item unconsumed by this consumer number, AFTER the previously
 * found result (passed in through prevfnd).  On a successful find *fnd will
 * be set to point to memory that the caller must free.  The actual item data
 * will be at ((const char *)*fnd) + *fnddtaoff). */
int bdb_queue_get(bdb_state_type *bdb_state, int consumer,
                  const struct bdb_queue_cursor *prevcursor, void **fnd,
                  size_t *fnddtalen, size_t *fnddtaoff,
                  struct bdb_queue_cursor *fndcursor, unsigned int *epoch,
                  int *bdberr);

/* Get the genid of a queue item that was retrieved by bdb_queue_get() */
unsigned long long bdb_queue_item_genid(const void *dta);

/* Call a callback function for each item on the queue.  The parameters to the
 * callback are: consumer number, item length, epoch time it was added,
 * userptr. */
enum {
    /* queue walk callback return codes */
    BDB_QUEUE_WALK_CONTINUE = 0,
    BDB_QUEUE_WALK_STOP = 1,
    BDB_QUEUE_WALK_STOP_CONSUMER = 2,

    /* flags to affect the behaviour of the walkback function */
    BDB_QUEUE_WALK_KNOWN_CONSUMERS_ONLY = 1,
    BDB_QUEUE_WALK_FIRST_ONLY = 2,
    BDB_QUEUE_WALK_RESTART = 4
};
typedef int (*bdb_queue_walk_callback_t)(int consumern, size_t item_length,
                                         unsigned int epoch, void *userptr);
int bdb_queue_walk(bdb_state_type *bdb_state, int flags, bbuint32_t *lastitem,
                   bdb_queue_walk_callback_t callback, void *userptr,
                   int *bdberr);

/* debug aid - dump the entire queue */
int bdb_queue_dump(bdb_state_type *bdb_state, FILE *out, int *bdberr);

/* consume a queue item previously found by bdb_queue_get. */
int bdb_queue_consume(bdb_state_type *bdb_state, tran_type *tran, int consumer,
                      const void *prevfnd, int *bdberr);

/* work out the best page size to use for the given average item size */
int bdb_queue_best_pagesize(int avg_item_sz);

/* Get info about a previously found item. */
void bdb_queue_get_found_info(const void *fnd, size_t *dtaoff, size_t *dtalen);

/* Get queue stats */
const struct bdb_queue_stats *bdb_queue_get_stats(bdb_state_type *bdb_state);

/* dump dta contents of bdb_handle to stream sb */
int bdb_dumpdta(bdb_state_type *bdb_handle, SBUF2 *sb, int *bdberr);

/* debug dump routines */
void bdb_dumpit(bdb_state_type *bdb_state);
void bdb_bulkdumpit(bdb_state_type *bdb_state);

/*
  compare seqnum1 with seqnum2.
   returns 0  if seqnum1 == seqnum2
           1  if seqnum1 >  seqnum2
           -1 if seqnum1 <  seqnum2
           */
int bdb_seqnum_compare(void *inbdb_state, seqnum_type *seqnum1,
                       seqnum_type *seqnum2);
char *bdb_format_seqnum(const seqnum_type *seqnum, char *buf, size_t bufsize);
int bdb_get_seqnum(bdb_state_type *bdb_state, seqnum_type *seqnum);
void bdb_make_seqnum(seqnum_type *seqnum, uint32_t logfile, uint32_t logbyte);

int bdb_wait_for_seqnum_from_all(bdb_state_type *bdb_state,
                                 seqnum_type *seqnum);
int bdb_wait_for_seqnum_from_node(bdb_state_type *bdb_state,
                                  seqnum_type *seqnum, const char *host);
int bdb_wait_for_seqnum_from_node_timeout(bdb_state_type *bdb_state,
                                          seqnum_type *seqnum, const char *host,
                                          int timeoutms);
int bdb_wait_for_seqnum_from_all_timeout(bdb_state_type *bdb_state,
                                         seqnum_type *seqnum, int timeoutms);
int bdb_wait_for_seqnum_from_room(bdb_state_type *bdb_state,
                                  seqnum_type *seqnum);
int bdb_wait_for_seqnum_from_all_adaptive(bdb_state_type *bdb_state,
                                          seqnum_type *seqnum, uint64_t txnsize,
                                          int *timeoutms);

int bdb_wait_for_seqnum_from_all_adaptive_newcoh(bdb_state_type *bdb_state,
                                                 seqnum_type *seqnum,
                                                 uint64_t txnsize,
                                                 int *timeoutms);

int bdb_wait_for_seqnum_from_n(bdb_state_type *bdb_state, seqnum_type *seqnum,
                               int n);

/* returns 1 if you are the master, 0 if you are not */
int bdb_amimaster(bdb_state_type *bdb_handle);

/* returns nodeid of master, -1 if there is no master */
char *bdb_whoismaster(bdb_state_type *bdb_handle);

/* get current sanc list.  pass in size of array.  returns number of sanc
 * nodes (may be > passed in list length). */
int bdb_get_sanc_list(bdb_state_type *bdb_state, int max_nodes,
                      const char *nodes[REPMAX]);

/* return the highest rrn in the db */
int bdb_get_highest_rrn(bdb_state_type *bdb_handle, int *rrn, int *bdberr);

int bdb_get_lua_highest(tran_type *trans, /* transaction to use, may be NULL */
                        const char *sp_name,
                        int *lua_vers, /* will be set to the highest version*/
                        int max_lua_vers, int *bdberr);

int bdb_get_sp_name(tran_type *trans, /* transaction to use, may be NULL */
                    const char *sp_name,
                    char *new_sp_name, /* will be set to the highest version*/
                    int *bdberr);

/* return 1 if this key is update-able, 0 otherwise */
int bdb_ix_updk_allowed(bdb_state_type *bdb_handle, int ixnum);

void bdb_process_user_command(bdb_state_type *bdb_handle, char *line, int lline,
                              int st);

void bdb_short_netinfo_dump(FILE *out, bdb_state_type *bdb_state);

/* DEBUG ONLY ROUTINES */
/* Fetch record by RRN.  FOR DEBUGGING ONLY!!!
   Fetched result goes into data.  Length of data fetched written to
   found_data_len.
   Returns 0 for success.
   If size > data_len, returns 1 (size still written to found_data_len)
   2 is a misc bdb error code.
 */
int bdb_fetch_by_rrn(bdb_state_type *bdb_handle, int rrn, void *data,
                     int data_len, int *found_data_len);
/* Caller is responsible for freeing *data.  Offset of data is returned in
 * *found_data_off.  Returns 0 for success, -1 for not found, -2 for error. */
int bdb_fetch_n_by_rrn(bdb_state_type *bdb_handle, int dtan, int rrn,
                       void **data, int *found_data_len, int *found_data_off,
                       unsigned long long *genid);
/* Dumps a .dta file to stream given. */
int bdb_dump_dta_file_n(bdb_state_type *bdb_state, int dtanum, FILE *out);
/* END DEBUG ONLY ROUTINES */

/* read.c */
int bdb_fetch_next_genids(bdb_state_type *bdb_state, int ixnum, int ixlen,
                          unsigned char *key, unsigned long long *genids,
                          int numgenids, int *num_genids_gotten, int *bdberr);

void bdb_set_io_control(void (*start)(), void (*cmplt)());

void bdb_get_iostats(int *n_reads, int *l_reads, int *n_writes, int *l_writes);

int bdb_rebuild_done(bdb_state_type *bdb_handle);

/* force a flush to disk of all in memory stuff */
int bdb_flush(bdb_state_type *bdb_handle, int *bdberr);

/* force a flush to disk of all in memory stuff , but don't force a checkpoint
 */
int bdb_flush_noforce(bdb_state_type *bdb_handle, int *bdberr);

int bdb_purge_freelist(bdb_state_type *bdb_handle, int *bdberr);

/* close the underlying files used byt the bdb_handle */
int bdb_close_only(bdb_state_type *bdb_handle, int *bdberr);

/* you must call bdb_close_only before a rename.
   if you want to rename a file that is in use and continue to use it, you
   need to:
   1) bdb_close_only.  2) begin tran. 3) rename 4) commit 5) bdb_open_again.
*/
int bdb_rename(bdb_state_type *bdb_handle, tran_type *tran, char newtablename[],
               int *bdberr);
int bdb_rename_data(bdb_state_type *bdb_state, tran_type *tran,
                    char newtablename[], int fromdtanum, int todtanum,
                    int *bdberr);
int bdb_rename_ix(bdb_state_type *bdb_state, tran_type *tran,
                  char newtablename[], int fromixnum, int toixnum, int *bdberr);
int bdb_rename_name(bdb_state_type *bdb_state, char newtablename[],
                    int *bdberr);

/* rename all the blob files for dynamic upgrade to blobstripe */
int bdb_rename_blob1(bdb_state_type *bdb_state, tran_type *tran,
                     unsigned long long *genid, int *bdberr);

/* Set the genid timestamp at which a blobstripe conversion was done.  This is
 * necessary to find records that were inserted pre-blobstripe. */
void bdb_set_blobstripe_genid(bdb_state_type *bdb_state,
                              unsigned long long genid);

/* set various options for the ondisk header. */
void bdb_set_odh_options(bdb_state_type *bdb_state, int odh, int compression,
                         int blob_compression);

void bdb_get_compr_flags(bdb_state_type *bdb_state, int *odh, int *compr,
                         int *blob_compr);

/* delete a table from disk.  must already be closed but NOT freed */
int bdb_del(bdb_state_type *bdb_state, tran_type *tran, int *bdberr);
int bdb_del_temp(bdb_state_type *bdb_state, tran_type *tran, int *bdberr);
int bdb_del_data(bdb_state_type *bdb_state, tran_type *tran, int dtanum,
                 int *bdberr);
int bdb_del_ix(bdb_state_type *bdb_state, tran_type *tran, int ixnum,
               int *bdberr);

int bdb_del_unused_files(bdb_state_type *bdb_state, int *bdberr);
int bdb_del_unused_files_tran(bdb_state_type *bdb_state, tran_type *tran,
                              int *bdberr);
int bdb_list_unused_files(bdb_state_type *bdb_state, int *bdberr, char *powner);
int bdb_list_unused_files_tran(bdb_state_type *bdb_state, tran_type *tran,
                               int *bdberr, char *powner);

/* make new stripes */
int bdb_create_stripes(bdb_state_type *bdb_state, int newdtastripe,
                       int newblobstripe, int *bdberr);

/* re-open bdb handle that has been bdb_close_only'ed
   as master/client depending on how it used to be */
int bdb_open_again(bdb_state_type *bdb_handle, int *bdberr);
int bdb_open_again_tran(bdb_state_type *bdb_state, tran_type *tran,
                        int *bdberr);

/* destroy resources related to bdb_handle.  assumes that bdb_close_only
   was called */
int bdb_free(bdb_state_type *bdb_handle, int *bdberr);
int bdb_free_and_replace(bdb_state_type *bdb_handle, bdb_state_type *replace,
                         int *bdberr);

int bdb_add_data_with_rrn(bdb_state_type *bdb_state, void *dta, int rrn,
                          unsigned long long genid, int dtalen, int *bdberr);

int bdb_zap_freerec(bdb_state_type *bdb_handle, int *bdberr);

/* temptables */
enum { BDB_TEMP_TABLE_DONT_USE_INMEM = 1 };
struct temp_table;
struct temp_cursor;
struct temp_table *bdb_temp_table_create(bdb_state_type *bdb_state,
                                         int *bdberr);
struct temp_table *bdb_temp_list_create(bdb_state_type *bdb_state, int *bdberr);
struct temp_table *bdb_temp_hashtable_create(bdb_state_type *bdb_state,
                                             int *bdberr);
struct temp_table *bdb_temp_table_create_flags(bdb_state_type *bdb_state,
                                               int flags, int *bdberr);

int bdb_temp_table_close(bdb_state_type *bdb_state, struct temp_table *table,
                         int *bdberr);

int bdb_temp_table_truncate(bdb_state_type *bdb_state, struct temp_table *tbl,
                            int *bdberr);

struct temp_cursor *bdb_temp_table_cursor(bdb_state_type *bdb_state,
                                          struct temp_table *table,
                                          void *usermem, int *bdberr);
int bdb_temp_table_close_cursor(bdb_state_type *bdb_state,
                                struct temp_cursor *cursor, int *bdberr);

int bdb_temp_table_insert(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                          void *key, int keylen, void *data, int dtalen,
                          int *bdberr);
int bdb_temp_table_update(bdb_state_type *bdb_state, struct temp_cursor *cur,
                          void *key, int keylen, void *data, int dtalen,
                          int *bdberr);
int bdb_temp_table_delete(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                          int *bdberr);

int bdb_temp_table_first(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                         int *bdberr);
int bdb_temp_table_last(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int *bdberr);
int bdb_temp_table_next(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int *bdberr);
int bdb_temp_table_prev(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int *bdberr);
int bdb_temp_table_move(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int how, int *bdberr);
int bdb_temp_table_next_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cursor, int *bdberr);
int bdb_temp_table_prev_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cursor, int *bdberr);

void *bdb_temp_table_get_usermem(struct temp_table *table);
void bdb_temp_table_set_user_data(struct temp_cursor *cur, void *usermem);

int bdb_temp_table_keysize(struct temp_cursor *cursor);
int bdb_temp_table_datasize(struct temp_cursor *cursor);
void *bdb_temp_table_key(struct temp_cursor *cursor);
void *bdb_temp_table_data(struct temp_cursor *cursor);

typedef int (*tmptbl_cmp)(void *, int, const void *, int, const void *);
void bdb_temp_table_set_cmp_func(struct temp_table *table, tmptbl_cmp);

int bdb_temp_table_find(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        const void *key, int keylen, void *unpacked,
                        int *bdberr);
int bdb_temp_table_find_exact(bdb_state_type *bdb_state,
                              struct temp_cursor *cursor, void *key, int keylen,
                              int *bdberr);
void bdb_temp_table_reset_datapointers(struct temp_cursor *cur);

void *bdb_temp_table_get_cur(struct temp_cursor *skippy);

int bdb_reinit(bdb_state_type *bdb_state, tran_type *tran, int *bdberr);

void bdb_get_cache_stats(bdb_state_type *bdb_state, uint64_t *hits,
                         uint64_t *misses, uint64_t *reads, uint64_t *writes,
                         uint64_t *thits, uint64_t *tmisses);
void bdb_thread_event(bdb_state_type *bdb_state, int event);

void bdb_stripe_get(bdb_state_type *bdb_state);
void bdb_stripe_done(bdb_state_type *bdb_state);

int bdb_count(bdb_state_type *bdb_state, int *bdberr);

struct bdb_temp_hash *bdb_temp_hash_create(bdb_state_type *bdb_state,
                                           char *tmpname, int *bdberr);
struct bdb_temp_hash *bdb_temp_hash_create_cache(bdb_state_type *bdb_state,
                                                 int cachekb, char *tmpname,
                                                 int *bdberr);
int bdb_temp_hash_destroy(bdb_temp_hash *h);
int bdb_temp_hash_insert(bdb_temp_hash *h, void *key, int keylen, void *dta,
                         int dtalen);
int bdb_temp_hash_lookup(bdb_temp_hash *h, void *key, int keylen, void *dta,
                         int *dtalen, int maxlen);

bulk_dump *bdb_start_fstdump(bdb_state_type *bdb_state, int *bdberr);
int bdb_next_fstdump(bulk_dump *dmp, void *buf, int sz, int *bdberr);
int bdb_close_fstdump(bulk_dump *dmp);
int bdb_fstdumpdta(bdb_state_type *bdb_state, SBUF2 *sb, int *bdberr);

/* fast dump and callback to convert records from ondisk to requested tag.
 * callback returns 0 for ok, -1 for could not convert. */
typedef int (*bdb_fstdumpdta_callback_t)(void *rec, size_t reclen,
                                         void *clientrec, size_t clientreclen,
                                         void *userptr, void *userptr2,
                                         const char *tzname, uint8_t ver,
                                         int flags);
int bdb_fstdumpdta_sendsz(bdb_state_type *bdb_state, SBUF2 *sb,
                          size_t sendrecsz,
                          bdb_fstdumpdta_callback_t convert_callback,
                          int callback_flags, void *userptr, void *userptr2,
                          int timeoutms, int close_cursor, int *bdberr,
                          const char *tzname, int getgenids);

/* Get the length of the first data item/first item in given index in this
 * table.  This can be used by
 * the db on startup as a sanity check to make sure that the length of the
 * stored data is the same as that required by the schema.  This is, of
 * course, a huge asusmption that the rest of the data will be of a consistent
 * length, and it doesn't help if we have the right data length but wrong
 * schema, but it catches a lot of problems easily. */
int bdb_get_first_data_length(bdb_state_type *bdb_state, int *bdberr);
int bdb_get_first_index_length(bdb_state_type *bdb_state, int ixnum,
                               int *bdberr);

int bdb_truncate(bdb_state_type *bdb_state, int *bdberr);

void bdb_start_request(bdb_state_type *bdb_state);
void bdb_end_request(bdb_state_type *bdb_state);
void bdb_start_exclusive_request(bdb_state_type *bdb_state);

dtadump *bdb_dtadump_start(bdb_state_type *bdb_state, int *bdberr, int is_blob,
                           int nr);
int bdb_dtadump_next(bdb_state_type *bdb_state, dtadump *dump, void **dta,
                     int *len, int *rrn, unsigned long long *genid,
                     uint8_t *ver, int *bdberr);
void bdb_dtadump_done(bdb_state_type *bdb_state, struct dtadump *dump);
int get_nr_dtastripe_files(bdb_state_type *bdb_state);

unsigned long long bdb_get_timestamp(bdb_state_type *bdb_state);
unsigned long long bdb_genid_to_host_order(unsigned long long genid_net_order);
unsigned long long bdb_increment_slot(bdb_state_type *bdb_state,
                                      unsigned long long a);
unsigned long long bdb_mask_stripe(bdb_state_type *bdb_state,
                                   unsigned long long a);

int bdb_validate_compression_alg(int alg);

void bdb_set_os_log_level(int level);
void bdb_log_berk_tables(bdb_state_type *bdb_state);

int bdb_sync_cluster(bdb_state_type *bdb_state, int sync_all);

int bdb_is_an_unconnected_master(bdb_state_type *bdb_state);
void bdb_transfermaster(bdb_state_type *bdb_state);
void bdb_losemaster(bdb_state_type *bdb_state);
void bdb_transfermaster_tonode(bdb_state_type *bdb_state, char *tohost);

void bdb_exiting(bdb_state_type *bdb_state);

int bdb_fetch_last_key_tran(bdb_state_type *bdb_state, tran_type *tran,
                            int write, int idx, int keylen, void *fndkey,
                            int *fndlen, int *bdberr);

int bdb_rowlock(bdb_state_type *bdb_state, tran_type *tran,
                unsigned long long genid, int exclusive, int *bdberr);

int bdb_get_low_headroom_count(bdb_state_type *bdb_state);

enum { BDB_LOCK_READ, BDB_LOCK_WRITE };

int bdb_get_locker(bdb_state_type *bdb_state, unsigned int *lid);
int bdb_lock_row(bdb_state_type *bdb_state, unsigned int lid,
                 unsigned long long genid);
int bdb_release_locker(bdb_state_type *bdb_state, unsigned int lid);

extern int gbl_bb_berkdb_enable_thread_stats;
extern int gbl_bb_berkdb_enable_lock_timing;
extern int gbl_bb_berkdb_enable_memp_timing;
extern int gbl_bb_berkdb_enable_memp_pg_timing;
extern int gbl_bb_berkdb_enable_shalloc_timing;
void bdb_reset_thread_stats(void);
const struct bdb_thread_stats *bdb_get_thread_stats(void);
const struct bdb_thread_stats *bdb_get_process_stats(void);

/* Format and print the thread stats.  printfn() is a function which accepts
 * a line to print (\n\0 terminated) and a context pointer. Its return value
 * is ignored.  bdb_fprintf_stats is a convenience wrapper which uses fputs()
 * as the printing function. */
void bdb_print_stats(const struct bdb_thread_stats *st, const char *prefix,
                     int (*printfn)(const char *, void *), void *context);
void bdb_fprintf_stats(const struct bdb_thread_stats *st, const char *prefix,
                       FILE *out);

int bdb_find_oldest_genid(bdb_state_type *bdb_state, tran_type *tran,
                          int stripe, void *rec, int *reclen, int maxlen,
                          unsigned long long *genid, uint8_t *ver, int *bdberr);
int bdb_find_newest_genid(bdb_state_type *bdb_state, tran_type *tran,
                          int stripe, void *rec, int *reclen, int maxlen,
                          unsigned long long *genid, uint8_t *ver, int *bdberr);

int bdb_genid_timestamp(unsigned long long genid);
unsigned long long bdb_recno_to_genid(int recno);
int bdb_genid_is_recno(bdb_state_type *bdb_state, unsigned long long genid);

int bdb_genid_exists(bdb_state_type *bdb_state, unsigned long long genid,
                     int *bdberr);

unsigned long long bdb_get_current_lsn(bdb_state_type *bdb_state,
                                       unsigned int *file,
                                       unsigned int *offset);

int bdb_set_tran_lowpri(bdb_state_type *bdb_state, tran_type *tran);

int bdb_am_i_coherent(bdb_state_type *bdb_state);

int bdb_get_num_notcoherent(bdb_state_type *bdb_state);
void bdb_get_notcoherent_list(bdb_state_type *bdb_state,
                              const char *nodes_list[REPMAX], size_t max_nodes,
                              int *num_notcoherent, int *since_epoch);
int bdb_is_skip(bdb_state_type *bdb_state, int node);
void set_skip(bdb_state_type *bdb_state, int node);
void clear_skip(bdb_state_type *bdb_state, int node);

void bdb_register_rtoff_callback(bdb_state_type *bdb_state, void (*func)(void));

/* Return the number of seconds that the current checkpoint has been running
 * for, or 0 if no checkpoint is currently running. */
int bdb_get_checkpoint_time(bdb_state_type *bdb_state);

int bdb_have_llmeta();
int bdb_llmeta_open(char name[], char dir[], bdb_state_type *parent_bdb_handle,
                    int create_override, int *bdberr);
int bdb_llmeta_set_tables(tran_type *input_trans, char **tblnames,
                          const int *dbnums, int numdbs, int *bdberr);
int bdb_llmeta_get_tables(tran_type *input_trans, char **tblnames, int *dbnums,
                          size_t maxnumtbls, int *fndnumtbls, int *bdberr);
bdb_state_type *bdb_llmeta_bdb_state(void);

int bdb_append_file_version(char *str_buf, size_t buflen,
                            unsigned long long version_num, int *bdberr);
int bdb_unappend_file_version(bdb_state_type *bdb_state, int *bdberr);
const char *bdb_unprepend_new_prefix(const char *tablename, int *bdberr);

int bdb_get_pagesize_data(bdb_state_type *bdb_state, tran_type *tran,
                          int *pagesize, int *bdberr);
int bdb_get_pagesize_blob(bdb_state_type *bdb_state, tran_type *tran,
                          int *pagesize, int *bdberr);
int bdb_get_pagesize_index(bdb_state_type *bdb_state, tran_type *tran,
                           int *pagesize, int *bdberr);

int bdb_get_pagesize_alldata(tran_type *tran, int *pagesize, int *bdberr);
int bdb_get_pagesize_allblob(tran_type *tran, int *pagesize, int *bdberr);
int bdb_get_pagesize_allindex(tran_type *tran, int *pagesize, int *bdberr);

int bdb_set_pagesize_data(bdb_state_type *bdb_state, tran_type *tran,
                          int pagesize, int *bdberr);
int bdb_set_pagesize_blob(bdb_state_type *bdb_state, tran_type *tran,
                          int pagesize, int *bdberr);
int bdb_set_pagesize_index(bdb_state_type *bdb_state, tran_type *tran,
                           int pagesize, int *bdberr);

int bdb_set_pagesize_alldata(tran_type *tran, int pagesize, int *bdberr);
int bdb_set_pagesize_allblob(tran_type *tran, int pagesize, int *bdberr);
int bdb_set_pagesize_allindex(tran_type *tran, int pagesize, int *bdberr);

int bdb_new_file_version_data(bdb_state_type *bdb_state, tran_type *tran,
                              int dtanum, unsigned long long version_num,
                              int *bdberr);
int bdb_new_file_version_index(bdb_state_type *bdb_state, tran_type *tran,
                               int ixnum, unsigned long long version_num,
                               int *bdberr);
int bdb_new_file_version_all(bdb_state_type *bdb_state, tran_type *input_tran,
                             int *bdberr);
int bdb_new_file_version_table(bdb_state_type *bdb_state, tran_type *tran,
                               unsigned long long version_num, int *bdberr);

int bdb_get_file_version_data(bdb_state_type *bdb_state, tran_type *tran,
                              int dtanum, unsigned long long *version_num,
                              int *bdberr);
int bdb_get_file_version_index(bdb_state_type *bdb_state, tran_type *tran,
                               int ixnum, unsigned long long *version_num,
                               int *bdberr);
int bdb_get_file_version_table(bdb_state_type *bdb_state, tran_type *tran,
                               unsigned long long *version_num, int *bdberr);

int bdb_del_file_versions(bdb_state_type *bdb_state, tran_type *input_trans,
                          int *bdberr);

int bdb_new_csc2(tran_type *input_trans, const char *db_name, int csc2_vers,
                 char *schema, int *bdberr);
int bdb_get_csc2(tran_type *tran, const char *db_name, int csc2_vers,
                 char **schema, int *bdberr);
int bdb_get_csc2_highest(tran_type *trans, const char *db_name, int *csc2_vers,
                         int *bdberr);

int bdb_get_new_prefix(char *buf, size_t buflen, int *bdberr);

int bdb_file_version_change_dtanum(bdb_state_type *bdb_state, tran_type *tran,
                                   int fromdtanum, int todtanum, int *bdberr);
int bdb_file_version_change_ixnum(bdb_state_type *bdb_state, tran_type *tran,
                                  int fromixnum, int toixnum, int *bdberr);

int bdb_commit_temp_file_version_data(bdb_state_type *bdb_state,
                                      tran_type *tran, int dtanum, int *bdberr);
int bdb_commit_temp_file_version_index(bdb_state_type *bdb_state,
                                       tran_type *tran, int ixnum, int *bdberr);
int bdb_commit_temp_file_version_all(bdb_state_type *bdb_state, tran_type *tran,
                                     int *bdberr);

int bdb_bulk_import_copy_cmd_add_tmpdir_filenames(
    bdb_state_type *bdb_state, unsigned long long src_data_genid,
    unsigned long long dst_data_genid,
    const unsigned long long *p_src_index_genids,
    const unsigned long long *p_dst_index_genids, size_t num_index_genids,
    const unsigned long long *p_src_blob_genids,
    const unsigned long long *p_dst_blob_genids, size_t num_blob_genids,
    char *outbuf, size_t buflen, int *bdberr);

int bdb_start_file_versioning_table(bdb_state_type *bdb_state, tran_type *tran,
                                    int *bdberr);
void bdb_remove_prefix(bdb_state_type *bdb_state);

void *bdb_del_list_new(int *bdberr);
int bdb_del_list_add_data(bdb_state_type *bdb_state, tran_type *tran,
                          void *list, int dtanum, int *bdberr);
int bdb_del_list_add_index(bdb_state_type *bdb_state, tran_type *tran,
                           void *list, int ixnum, int *bdberr);
int bdb_del_list_add_all(bdb_state_type *bdb_state, tran_type *tran, void *list,
                         int *bdberr);
int bdb_del_list(bdb_state_type *bdb_state, void *list, int *bdberr);
int bdb_del_list_free(void *list, int *bdberr);

int bdb_set_in_schema_change(tran_type *input_trans, const char *db_name,
                             void *schema_change_data,
                             size_t schema_change_data_len, int *bdberr);
int bdb_get_in_schema_change(const char *db_name, void **schema_change_data,
                             size_t *schema_change_data_len, int *bdberr);

int bdb_set_high_genid(tran_type *input_trans, const char *db_name,
                       unsigned long long genid, int *bdberr);
int bdb_set_high_genid_stripe(tran_type *input_trans, const char *db_name,
                              int stripe, unsigned long long genid,
                              int *bdberr);
int bdb_clear_high_genid(tran_type *input_trans, const char *db_name,
                         int num_stripes, int *bdberr);
int bdb_get_high_genid(const char *db_name, int stripe,
                       unsigned long long *genid, int *bdberr);

void bdb_scdone_start(bdb_state_type *bdb_state);
void bdb_scdone_end(bdb_state_type *bdb_state);

unsigned long long increment_seq(unsigned long long crt);

void bdb_get_cur_lsn_str(bdb_state_type *bdb_state, uint64_t *lsnbytes,
                         char *lsnstr, size_t len);

unsigned long long bdb_temp_table_new_rowid(struct temp_table *tbl);

int bdb_temp_table_put(bdb_state_type *bdb_state, struct temp_table *tbl,
                       void *key, int keylen, void *data, int dtalen,
                       void *unpacked, int *bdberr);

void bdb_get_cur_lsn_str(bdb_state_type *bdb_state, uint64_t *lsnbytes,
                         char *lsnstr, size_t len);

void bdb_get_cur_lsn_str_node(bdb_state_type *bdb_state, uint64_t *lsnbytes,
                              char *lsnstr, size_t len, char *host);

/* Given a berkeley db lockid (i.e. some bytes of data), try to get
 * a human readable name for it.  This is based on __lock_printlock
 * in lock/lock_stat.c */
void bdb_lock_name(bdb_state_type *bdb_state, char *s, size_t slen,
                   void *lockid, size_t lockid_len);

int bdb_check_pageno(bdb_state_type *bdb_state, uint32_t pgno);

int bdb_check_genid_is_newer(bdb_state_type *bdb_state,
                             unsigned long long genid,
                             unsigned long long context);

int bdb_lock_desired(bdb_state_type *bdb_state);

void bdb_bdblock_print(bdb_state_type *bdb_state, char *str);

/* disable/re-enable inplace update code during schema change on a bit */
enum { BDB_DISABLE_INPLACE_UPDATES = 1, BDB_ENABLE_INPLACE_UPDATES = 0 };

void bdb_inplace_sc_disable_flag(int sc);

int bdb_lock_stripe_read(bdb_state_type *bdb_state, int stripe,
                         tran_type *tran);
int bdb_lock_stripe_write(bdb_state_type *bdb_state, int stripe,
                          tran_type *tran);

/* init/destroy bdb osql support for snapshot/serializable sql transactions */
int bdb_osql_init(int *bdberr);
int bdb_osql_destroy(int *bdberr);

void bdb_get_rep_stats(bdb_state_type *bdb_state,
                       unsigned long long *msgs_processed,
                       unsigned long long *msgs_sent,
                       unsigned long long *txns_applied,
                       unsigned long long *retry, int *max_retry);

int bdb_get_index_filename(bdb_state_type *bdb_state, int ixnum, char *nameout,
                           int namelen, int *bdberr);
int bdb_get_data_filename(bdb_state_type *bdb_state, int stripe, int blob,
                          char *nameout, int namelen, int *bdberr);

int bdb_summarize_table(bdb_state_type *bdb_state, int ixnum, int comp_pct,
                        struct temp_table **outtbl, unsigned long long *outrecs,
                        unsigned long long *cmprecs, int *bdberr);

void bdb_bdblock_debug(void);
int bdb_env_init_after_llmeta(bdb_state_type *bdb_state);

int bdb_rowlocks_check_commit_physical(bdb_state_type *bdb_state,
                                       tran_type *tran, int blockop_count);
int bdb_is_rowlocks_transaction(tran_type *tran);

int bdb_get_sp_get_default_version(const char *sp_name, int *bdberr);
int bdb_set_sp_lua_source(bdb_state_type *bdb_state, tran_type *tran,
                          const char *sp_name, char *lua_file, int size,
                          int version, int *bdberr);
int bdb_get_sp_lua_source(bdb_state_type *bdb_state, tran_type *tran,
                          const char *sp_name, char **lua_file, int version,
                          int *size, int *bdberr);
int bdb_delete_sp_lua_source(bdb_state_type *bdb_state, tran_type *tran,
                             const char *sp_name, int lua_ver, int *bdberr);
int bdb_set_sp_lua_default(bdb_state_type *bdb_state, tran_type *tran,
                           char *sp_name, int lua_ver, int *bdberr);

int bdb_set_disable_plan_genid(bdb_state_type *bdb_state, tran_type *tran,
                               unsigned long long genid, unsigned int host,
                               int *bdberr);
int bdb_get_disable_plan_genid(bdb_state_type *bdb_state, tran_type *tran,
                               unsigned long long *genid, unsigned int *host,
                               int *bdberr);
int bdb_delete_disable_plan_genid(bdb_state_type *bdb_state, tran_type *tran,
                                  int *bdberr);

enum {
    ACCESS_INVALID = 0,
    ACCESS_READ = 1,
    ACCESS_WRITE = 2,
    ACCESS_DDL = 3,
    ACCESS_USERSCHEMA = 4
};

int bdb_tbl_access_write_set(bdb_state_type *bdb_state, tran_type *input_trans,
                             char *tblname, char *username, int *bdberr);
int bdb_tbl_access_write_get(bdb_state_type *bdb_state, tran_type *input_trans,
                             char *tblname, char *username, int *bdberr);

int bdb_tbl_access_read_set(bdb_state_type *bdb_state, tran_type *input_trans,
                            char *tblname, char *username, int *bdberr);
int bdb_tbl_access_read_get(bdb_state_type *bdb_state, tran_type *input_trans,
                            char *tblname, char *username, int *bdberr);

int bdb_tbl_access_userschema_set(bdb_state_type *bdb_state,
                                  tran_type *input_trans, const char *schema,
                                  const char *username, int *bdberr);

int bdb_tbl_access_userschema_get(bdb_state_type *bdb_state,
                                  tran_type *input_trans, const char *username,
                                  char *userschema, int *bdberr);

int bdb_tbl_op_access_set(bdb_state_type *bdb_state, tran_type *input_trans,
                          int command_type, const char *tblname, const char *username,
                          int *bdberr);

int bdb_tbl_op_access_get(bdb_state_type *bdb_state, tran_type *input_trans,
                          int command_type, const char *tblname, const char *username,
                          int *bdberr);

int bdb_tbl_op_access_delete(bdb_state_type *bdb_state, tran_type *input_trans,
                             int command_type, const char *tblname, const char *username,
                             int *bdberr);

int bdb_authentication_set(bdb_state_type *bdb_state, tran_type *input_trans, int enable,
                           int *bdberr);
int bdb_authentication_get(bdb_state_type *bdb_state, tran_type *tran,
                           int *bdberr);
int bdb_accesscontrol_tableXnode_get(bdb_state_type *bdb_state, tran_type *tran,
                                     int *bdberr);

int bdb_user_password_set(tran_type *, char *user, char *passwd);
int bdb_user_password_check(char *user, char *passwd, int *valid_user);
int bdb_user_password_delete(tran_type *tran, char *user);
int bdb_user_get_all(char ***users, int *num);

int bdb_verify(
    SBUF2 *sb, bdb_state_type *bdb_state,
    int (*formkey_callback)(void *parm, void *dta, void *blob_parm, int ix,
                            void *keyout, int *keysz),
    int (*get_blob_sizes_callback)(void *parm, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs),
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver),
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz,
                                    int blobno),
    void (*free_blob_buffer_callback)(void *parm),
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta,
                                                  void *blob_parm),
    void *callback_parm, 
    int (*lua_callback)(void *, const char *), void *lua_params, 
    void *callback_blob_buf, int progress_report_seconds,
    int attempt_fix);

void bdb_set_instant_schema_change(bdb_state_type *bdb_state, int isc);
void bdb_set_inplace_updates(bdb_state_type *bdb_state, int ipu);
void bdb_set_csc2_version(bdb_state_type *bdb_state, uint8_t version);

int bdb_get_active_stripe(bdb_state_type *bdb_state);

void bdb_set_datacopy_odh(bdb_state_type *, int);

extern void bdb_dump_active_locks(bdb_state_type *bdb_state, FILE *out);

int bdb_add_rep_blob(bdb_state_type *bdb_state, tran_type *tran, int session,
                     int seqno, void *blob, int sz, int *bdberr);

const char *bdb_get_tmpdir(bdb_state_type *bdb_state);

int bdb_form_file_name(bdb_state_type *bdb_state, int is_data_file, int filenum,
                       int stripenum, unsigned long long version_num,
                       char *outbuf, size_t buflen);

void bdb_verify_dbreg(bdb_state_type *bdb_state);

int bdb_the_lock_desired(void);

int bdb_is_hashtable(struct temp_table *);

void analyze_set_headroom(uint64_t);

int bdb_is_open(bdb_state_type *bdb_state);

void bdb_checklock(bdb_state_type *);
void berkdb_set_max_rep_retries(int max);
void bdb_set_recovery(bdb_state_type *);
tran_type *bdb_tran_begin_set_retries(bdb_state_type *, tran_type *parent,
                                      int retries, int *bdberr);
void bdb_lockspeed(bdb_state_type *bdb_state);
int bdb_lock_table_write(bdb_state_type *bdb_state, tran_type *tran);
int bdb_reset_csc2_version(tran_type *trans, const char *dbname, int ver);
void bdb_set_skip(bdb_state_type *bdb_state, int node);
unsigned long long get_id(bdb_state_type *bdb_state);
int bdb_access_tbl_write_by_mach_get(bdb_state_type *bdb_state, tran_type *tran,
                                     char *table, int hostnum, int *bdberr);
int bdb_access_tbl_read_by_mach_get(bdb_state_type *bdb_state, tran_type *tran,
                                    char *table, int hostnum, int *bdberr);
int bdb_flush_up_to_lsn(bdb_state_type *bdb_state, unsigned file,
                        unsigned offset);
int bdb_set_parallel_recovery_threads(bdb_state_type *bdb_state, int nthreads);
unsigned long long bdb_get_commit_genid(bdb_state_type *bdb_state, void *plsn);
void bdb_set_commit_lsn_gen(bdb_state_type *bdb_state, const void *lsn, uint32_t gen);
unsigned long long bdb_get_commit_genid_generation(bdb_state_type *bdb_state,
                                                   void *plsn,
                                                   uint32_t *generation);
void bdb_set_commit_genid(bdb_state_type *bdb_state, unsigned long long context,
                          const uint32_t *generation, const void *plsn,
                          const void *args, unsigned int rectype);
unsigned long long bdb_gen_commit_genid(bdb_state_type *bdb_state,
                                        const void *plsn, uint32_t generation);

void udp_ping_ip(bdb_state_type *, char *ip);
void udp_ping_all(bdb_state_type *);
void udp_ping(bdb_state_type *, char *to);
void udp_prefault_all(bdb_state_type *bdb_state, unsigned int fileid,
                      unsigned int pgno);
void tcp_ping_all(bdb_state_type *);
void tcp_ping(bdb_state_type *, char *to);
void ping_all(bdb_state_type *);
void ping_node(bdb_state_type *, char *to);
netinfo_type *get_rep_netinfo(bdb_state_type *);

void udp_summary(void);
void udp_reset(netinfo_type *);

extern struct thdpool *gbl_udppfault_thdpool;
int udppfault_thdpool_init(void);

extern struct thdpool *gbl_pgcompact_thdpool;
int pgcompact_thdpool_init(void);

int get_dbnum_by_handle(bdb_state_type *bdb_state);
int send_myseqnum_to_master(bdb_state_type *, int nodelay);

const char *bdb_temp_table_filename(struct temp_table *);
void bdb_temp_table_flush(struct temp_table *);

int bdb_tran_free_shadows(bdb_state_type *bdb_state, tran_type *tran);

int bdb_gbl_pglogs_init(bdb_state_type *bdb_state);
int bdb_gbl_pglogs_mem_init(bdb_state_type *bdb_state);

int bdb_purge_unused_files(bdb_state_type *bdb_state, tran_type *tran,
                           int *bdberr);
int bdb_have_unused_files(void);

int bdb_nlocks_for_locker(bdb_state_type *bdb_state, int lid);

int bdb_llmeta_list_records(bdb_state_type *bdb_state, int *bdberr);

int bdb_have_ipu(bdb_state_type *bdb_state);

bdb_state_type *bdb_get_table_by_name(bdb_state_type *bdb_state, char *table);
int bdb_osql_check_table_version(bdb_state_type *bdb_state, tran_type *tran,
                                 int trak, int *bdberr);

void bdb_get_myseqnum(bdb_state_type *bdb_state, seqnum_type *seqnum);

void bdb_replace_handle(bdb_state_type *parent, int ix, bdb_state_type *handle);

int bdb_get_lock_counters(bdb_state_type *bdb_state, int64_t *deadlocks,
                          int64_t *waits);

int bdb_get_bpool_counters(bdb_state_type *bdb_state, int64_t *bpool_hits,
                           int64_t *bpool_misses);

int bdb_master_should_reject(bdb_state_type *bdb_state);

void bdb_berkdb_iomap_set(bdb_state_type *bdb_state, int onoff);

int bdb_berkdb_set_attr(bdb_state_type *bdb_state, char *attr, char *value,
                        int ivalue);
int bdb_berkdb_set_attr_after_open(bdb_attr_type *bdb_attr, char *attr,
                                   char *value, int ivalue);

void bdb_berkdb_dump_attrs(bdb_state_type *bdb_state, FILE *out);

int bdb_berkdb_blobmem_yield(bdb_state_type *bdb_state);

int calc_pagesize(int recsize);

int getpgsize(void *handle_);
void bdb_show_reptimes_compact(bdb_state_type *bdb_state);

void fill_dbinfo(void *dbinfo_response, bdb_state_type *bdb_state);

void bdb_disable_replication_time_tracking(bdb_state_type *bdb_state);
void bdb_set_key_compression(bdb_state_type *);
void bdb_print_compression_flags(bdb_state_type *);

int bdb_recovery_start_lsn(bdb_state_type *bdb_state, char *lsnout, int lsnlen);
/* We carefully pretend above bdb level that we don't know what an LSN looks
 * like.
 * Maintain the charade just a bit longer +---------V. */
int bdb_recovery_set_lsn(bdb_state_type *bdb_state, char *lsn);
/* Magic strings that correspond to no master and dupe master. BDB makes every
 * effort
 * to never use berkdb eid values, these map to DB_EID_INVALID,
 * DB_EID_BROADCAST, DB_EID_DUPMASTER */
extern char *bdb_master_dupe, *db_eid_broadcast, *db_eid_invalid;

int bdb_is_timestamp_recoverable(bdb_state_type *bdb_state, int32_t timestamp);

int bdb_get_analyzecoverage_table(tran_type *input_trans, const char *tbl_name,
                                  int *coveragevalue, int *bdberr);
int bdb_set_analyzecoverage_table(tran_type *input_trans, const char *tbl_name,
                                  int coveragevalue, int *bdberr);
int bdb_clear_analyzecoverage_table(tran_type *input_trans,
                                    const char *tbl_name, int *bdberr);

int bdb_get_analyzethreshold_table(tran_type *input_trans, const char *tbl_name,
                                   long long *threshold, int *bdberr);
int bdb_set_analyzethreshold_table(tran_type *input_trans, const char *tbl_name,
                                   long long threshold, int *bdberr);
int bdb_clear_analyzethreshold_table(tran_type *input_trans,
                                     const char *tbl_name, int *bdberr);

/* Rowlocks state */
enum {
    LLMETA_ROWLOCKS_DISABLED = 0,
    LLMETA_ROWLOCKS_ENABLED = 1,
    LLMETA_ROWLOCKS_ENABLED_MASTER_ONLY = 2,
    LLMETA_ROWLOCKS_DISABLED_TEMP_SC = 3,
    LLMETA_ROWLOCKS_STATE_MAX = 4
};

/* Genid format */
enum {
    LLMETA_GENID_ORIGINAL = 0 /* 32-bit timestamp, 16-bit dups */
    ,
    LLMETA_GENID_48BIT = 1 /* Increment-only / 48-bit ts+dup */
    ,
    LLMETA_GENID_FORMAT_MAX = 2
};

int bdb_get_rowlocks_state(int *rlstate, int *bdberr);
int bdb_set_rowlocks_state(tran_type *input_trans, int rlstate, int *bdberr);
int bdb_get_genid_format(uint64_t *genid_format, int *bdberr);
int bdb_set_genid_format(uint64_t genid_format, int *bdberr);

int bdb_get_table_csonparameters(const char *table, char **value, int *len);
int bdb_set_table_csonparameters(void *parent_tran, const char *table,
                                 const char *value, int len);
int bdb_del_table_csonparameters(void *parent_tran, const char *table);
int bdb_clear_table_parameter(void *parent_tran, const char *table,
                              const char *parameter);
int bdb_get_table_parameter(const char *table, const char *parameter,
                            char **value);
int bdb_set_table_parameter(void *parent_tran, const char *table,
                            const char *parameter, const char *value);

int bdb_disable_page_scan_for_table(bdb_state_type *bdb_state);
int bdb_enable_page_scan_for_table(bdb_state_type *bdb_state);
int bdb_get_page_scan_for_table(bdb_state_type *bdb_state);

int llmeta_get_curr_analyze_count(uint64_t *value);
int llmeta_set_curr_analyze_count(uint64_t value);
int llmeta_get_last_analyze_count(uint64_t *value);
int llmeta_set_last_analyze_count(uint64_t value);
int llmeta_get_last_analyze_epoch(uint64_t *value);
int llmeta_set_last_analyze_epoch(uint64_t value);

int bdb_osql_serial_check(bdb_state_type *bdb_state, void *ranges,
                          unsigned int *file, unsigned int *offset,
                          int regop_only);

int llmeta_set_tablename_alias(void *ptran, const char *tablename_alias,
                               const char *url, char **errstr);
char *llmeta_get_tablename_alias(const char *tablename_alias, char **errstr);
int llmeta_rem_tablename_alias(const char *tablename_alias, char **errstr);
void llmeta_list_tablename_alias(void);

int bdb_create_private_blkseq(bdb_state_type *bdb_state);
int bdb_blkseq_clean(bdb_state_type *bdb_state, uint8_t stripe);
int bdb_blkseq_insert(bdb_state_type *bdb_state, tran_type *tran, void *key,
                      int klen, void *data, int datalen, void **dtaout,
                      int *lenout);
int bdb_blkseq_find(bdb_state_type *bdb_state, tran_type *tran, void *key,
                    int klen, void **dtaout, int *lenout);
int bdb_blkseq_dumpall(bdb_state_type *bdb_state, uint8_t stripe);
int bdb_recover_blkseq(bdb_state_type *bdb_state);
int bdb_blkseq_dumplogs(bdb_state_type *bdb_state);
int bdb_blkseq_can_delete_log(bdb_state_type *bdb_state, int lognum);

/* low level calls to add things to a single btree for debugging and emergency
 * repair */
int bdb_llop_del(bdb_state_type *bdb_state, void *trans, int stripe,
                 int dtafile, int ix, void *key, int keylen, char **errstr);
int bdb_llop_add(bdb_state_type *bdb_state, void *trans, int raw, int stripe,
                 int dtafile, int ix, void *key, int keylen, void *data,
                 int datalen, void *dtacopy, int dtacopylen, char **errstr);
void *bdb_llop_find(bdb_state_type *bdb_state, void *trans, int raw, int stripe,
                    int dtafile, int ix, void *key, int keylen, int *fndlen,
                    uint8_t *ver, char **errmstr);

int bdb_panic(bdb_state_type *bdb_state);

int bdb_debug_logreq(bdb_state_type *bdb_state, int file, int offset);

/**
 *  Increment the TABLE VERSION ENTRY for table "bdb_state->name".
 *  If an entry doesn't exist, an entry with value 1 is created (default 0 means
 * non-existing)
 *
 */
int bdb_table_version_upsert(bdb_state_type *bdb_state, tran_type *tran,
                             int *bdberr);
/**
 *  Delete the TABLE VERSION ENTRY for table "bdb_state->name"
 *
 */
int bdb_table_version_delete(bdb_state_type *bdb_state, tran_type *tran,
                             int *bdberr);
/**
 *  Select the TABLE VERSION ENTRY for table "bdb_state->name".
 *  If an entry doesn't exist, version 0 is returned
 *
 */
int bdb_table_version_select(const char *name, tran_type *tran,
                             unsigned long long *version, int *bdberr);

void bdb_send_analysed_table_to_master(bdb_state_type *bdb_state, char *table);
/* get list of queues */
int bdb_llmeta_get_queues(char **queue_names, size_t max_queues,
                          int *fnd_queues, int *bdberr);
/* get info for a queue */
int bdb_llmeta_get_queue(char *qname, char **config, int *ndests, char ***dests,
                         int *bdberr);

/* manipulate queues */
int bdb_llmeta_add_queue(bdb_state_type *bdb_state, tran_type *tran,
                         char *queue, char *config, int ndests, char **dests,
                         int *bdberr);
int bdb_llmeta_alter_queue(bdb_state_type *bdb_state, tran_type *tran,
                           char *queue, char *config, int ndests, char **dests,
                           int *bdberr);
int bdb_llmeta_drop_queue(bdb_state_type *bdb_state, tran_type *tran,
                          char *queue, int *bdberr);

#include "bdb_queuedb.h"

/* get list of sequences */
int bdb_llmeta_get_sequence_names(char **sequence_names, size_t max_seqs,
                                  int *num_sequences, int *bdberr);

/* get attributes for a sequence */
int bdb_llmeta_get_sequence(char *name, long long *min_val, long long *max_val,
                            long long *increment, bool *cycle,
                            long long *start_val, long long *next_start_val,
                            long long *chunk_size, char *flags, int *bdberr);

/* manipulate sequences */
int bdb_llmeta_add_sequence(tran_type *tran, char *name, long long min_val,
                            long long max_val, long long increment, bool cycle,
                            long long start_val, long long next_start_val,
                            long long chunk_size, char flags, int *bdberr);

int bdb_llmeta_alter_sequence(tran_type *tran, char *name, long long min_val,
                              long long max_val, long long increment,
                              bool cycle, long long start_val,
                              long long next_start_val, long long chunk_size,
                              char flags, int *bdberr);

int bdb_llmeta_drop_sequence(tran_type *tran, char *name, int *bdberr);

int bdb_llmeta_get_sequence_chunk(tran_type *tran, char *name,
                                  long long min_val, long long max_val,
                                  long long increment, bool cycle,
                                  long long chunk_size, char *flags,
                                  long long *remaining_vals,
                                  long long start_val,
                                  long long *next_start_val, int *bdberr);

void lock_info_lockers(FILE *out, bdb_state_type *bdb_state);

const char *bdb_find_net_host(bdb_state_type *bdb_state, const char *host);

unsigned long long bdb_get_a_genid(bdb_state_type *bdb_state);

/* Return the timestamp of the replicants coherency lease */
uint64_t get_coherency_timestamp(void);

/* Return the next timestamp that the master is allowed to commit */
time_t next_commit_timestamp(void);

int bdb_genid_format(bdb_state_type *bdb_state);
int bdb_genid_set_format(bdb_state_type *bdb_state, int format);
int bdb_genid_allow_original_format(bdb_state_type *bdb_state);
int genid_contains_time(bdb_state_type *bdb_state);

int bdb_llmeta_get_lua_sfuncs(char ***, int *, int *bdberr);
int bdb_llmeta_add_lua_sfunc(char *, int *bdberr);
int bdb_llmeta_del_lua_sfunc(char *, int *bdberr);
int bdb_llmeta_get_lua_afuncs(char ***, int *, int *bdberr);
int bdb_llmeta_add_lua_afunc(char *, int *bdberr);
int bdb_llmeta_del_lua_afunc(char *, int *bdberr);

/* IO smoke test */
int bdb_watchdog_test_io(bdb_state_type *bdb_state);

int bdb_add_versioned_sp(char *name, char *version, char *src);
int bdb_get_versioned_sp(char *name, char *version, char **src);
int bdb_del_versioned_sp(char *name, char *version);

int bdb_set_default_versioned_sp(char *name, char *version);
int bdb_get_default_versioned_sp(char *name, char **version);
int bdb_del_default_versioned_sp(tran_type *tran, char *name);

int bdb_get_all_for_versioned_sp(char *name, char ***versions, int *num);
int bdb_get_default_versioned_sps(char ***names, int *num);
int bdb_get_versioned_sps(char ***names, int *num);

int bdb_check_user_tbl_access(bdb_state_type *bdb_state, char *user,
                              char *table, int access_type, int *bdberr);
int bdb_first_user_get(bdb_state_type *bdb_state, tran_type *tran,
                       char *key_out, char *user_out, int *isop, int *bdberr);
int bdb_next_user_get(bdb_state_type *bdb_state, tran_type *tran, char *key,
                      char *user_out, int *isop, int *bdberr);
int bdb_latest_commit_is_durable(void *bdb_state);
int bdb_is_standalone(void *dbenv, void *in_bdb_state);

uint32_t bdb_get_rep_gen(bdb_state_type *bdb_state);

typedef struct bias_info bias_info;
typedef int (*bias_cmp_t)(bias_info *, void *found);
typedef struct BtCursor BtCursor;
typedef struct UnpackedRecord UnpackedRecord;
struct bias_info {
    int bias;
    int dirLeft;
    int truncated;
    bias_cmp_t cmp;
    BtCursor *cur;
    UnpackedRecord *unpacked;
};

void bdb_set_fld_hints(bdb_state_type *, uint16_t *);

#endif
