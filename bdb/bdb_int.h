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

#ifndef __bdb_int_h__
#define __bdb_int_h__

#define restrict

/*#define RW_RRN_LOCK*/

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <build/db.h>
#include <bb_stdint.h>
#include <compile_time_assert.h>

#include <object_pool.h>
#include <list.h>
#include <plhash.h>
#include <thread_util.h>

#include "bdb_cursor.h"
#include "cursor_ll.h"
#include "bdb_access.h"

#include <compile_time_assert.h>
#include <epochlib.h>
#include <cheapstack.h>
#include <cdb2_constants.h>

#include "averager.h"
#include "intern_strings.h"
#include "bdb_schemachange.h"

#define MAXRECSZ (17 * 1024)
#define MAXKEYSZ (1024)

#define MAXTABLES 4096
#define MAXIX 64
#define NIL -1
#define MAXDTAFILES 16 /* primary data file + 15 blobs files */
#define MAXSTRIPE 16   /* max stripe factor */

/* Some additional error codes, chosen not to conflict with system codes
 * or with berkdb error codes.  Use bdb_strerror() to decode. */
#define DB_ODH_CORRUPT (-40000)    /* On disk header corrupt */
#define DB_UNCOMPRESS_ERR (-40001) /* Cannot inflate compressed rec */

#include "ix_return_codes.h"

#include "mem_bdb.h"
#include "mem_override.h"
#include "tunables.h"

/* Public ODH constants */
enum {
    ODH_UPDATEID_BITS = 12,
    ODH_LENGTH_BITS = 28,

    ODH_SIZE = 7, /* We may extend for larger headers in the future,
                     but the minimum size shall always be 7 bytes. */

    ODH_SIZE_RESERVE = 7, /* Callers wishing to provide a buffer into which
                             a record will be packed should allow this many
                             bytes on top of the record size for the ODH.
                             Right now this is the same as ODH_SIZE - one
                             day it may be the max possible ODH size if we
                             start adding fields. */

    ODH_FLAG_COMPR_MASK = 0x7
};

/* snapisol log ops */
enum log_ops { LOG_APPLY = 0, LOG_PRESCAN = 1, LOG_BACKFILL = 2 };

/* These are the fields of the ondisk header.  This is not the ondisk
 * representation but a convenient format for passing the header around in
 * our code. */
struct odh {
    uint32_t length;   /* actually only 28 bits of this can be used leading to
                          a max value of (1<<ODH_LENGTH_BITS)-1 */
    uint16_t updateid; /* actually only 12 bits of this can be used leading to
                          a max value of (1<<ODH_UPDATEID_BITS)-1 */
    uint8_t csc2vers;
    uint8_t flags;

    void *recptr; /* Some functions set this to point to the
                     decompressed record data. */
};

#ifndef MIN
#define MIN(a, b) (((a) < (b)) ? (a) : (b))
#endif

#ifndef MAX
#define MAX(a, b) (((a) > (b)) ? (a) : (b))
#endif

/* by trial and error it seems that for queue databases the available bytes for
 * record data is pagesize-32.  Can't seem to find an appropriate constant
 * in berkdb... */
#define QUEUE_PAGE_HEADER_SZ 32

void make_lsn(DB_LSN *logseqnum, unsigned int filenum, unsigned int offsetnum);

struct tran_table_shadows;
typedef struct tran_table_shadows tran_table_shadows_t;

typedef enum {
    TRANCLASS_BERK = 1,
    TRANCLASS_LOGICAL = 2,
    TRANCLASS_PHYSICAL = 3,
    TRANCLASS_READCOMMITTED = 4,
    TRANCLASS_SERIALIZABLE = 5,
    /* TRANCLASS_QUERYISOLATION = 6, */
    TRANCLASS_LOGICAL_NOROWLOCKS = 7, /* used in fetch.c for table locks */
    TRANCLASS_SOSQL = 8,
    TRANCLASS_SNAPISOL = 9
} tranclass_type;

#define PAGE_KEY                                                               \
    unsigned char fileid[DB_FILE_ID_LEN];                                      \
    db_pgno_t pgno;

#define PAGE_KEY_SIZE                                                          \
    (DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t))

struct lsn_list {
    DB_LSN lsn;
    LINKC_T(struct lsn_list) lnk;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};

struct commit_list {
    DB_LSN commit_lsn;
    unsigned long long logical_tranid;
    LINKC_T(struct commit_list) lnk;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};

struct lsn_commit_list {
    DB_LSN lsn;
    DB_LSN commit_lsn;
    LINKC_T(struct lsn_commit_list) lnk;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};

struct relink_list {
    db_pgno_t inh;
    DB_LSN lsn;
    LINKC_T(struct relink_list) lnk;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};

enum { PGLOGS_QUEUE_PAGE = 1, PGLOGS_QUEUE_RELINK = 2 };

struct pglogs_queue_key {
    LINKC_T(struct pglogs_queue_key) lnk;
    unsigned long long logical_tranid;
    int type;
    db_pgno_t pgno;
    db_pgno_t prev_pgno;
    db_pgno_t next_pgno;
    DB_LSN lsn;
    DB_LSN commit_lsn;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};

struct asof_cursor {
    unsigned char fileid[DB_FILE_ID_LEN];
    struct pglogs_queue_key *cur;
};

struct fileid_pglogs_queue {
    unsigned char fileid[DB_FILE_ID_LEN];
    int deleteme;
    pthread_rwlock_t queue_lk;
    LISTC_T(struct pglogs_queue_key) queue_keys;
};

// This is stored in a hash indexed by fileid.  All cursors pointed
// at a fileid maintain a pointer to the same memory.
struct pglogs_queue_cursor {
    unsigned char fileid[DB_FILE_ID_LEN];
    struct fileid_pglogs_queue *queue;
    struct pglogs_queue_key *last;
};

struct pglogs_queue_heads {
    int index;
    unsigned char **fileids;
};

struct page_logical_lsn_key {
    PAGE_KEY
    DB_LSN lsn;
    DB_LSN commit_lsn;
};

struct pglogs_key {
    PAGE_KEY
    LISTC_T(struct lsn_list) lsns;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};
#define PGLOGS_KEY_OFFSET (offsetof(struct pglogs_key, fileid))

struct pglogs_logical_key {
    PAGE_KEY
    LISTC_T(struct lsn_commit_list) lsns;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};
#define PGLOGS_LOGICAL_KEY_OFFSET (offsetof(struct pglogs_logical_key, fileid))

struct pglogs_relink_key {
    PAGE_KEY
    LISTC_T(struct relink_list) relinks;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
};
#define PGLOGS_RELINK_KEY_OFFSET (offsetof(struct pglogs_relink_key, fileid))

struct ltran_pglogs_key {
    unsigned long long logical_tranid;
    pthread_mutex_t pglogs_mutex;
    DB_LSN logical_commit_lsn; /* lsn of the physical commit of the logical
                                  transaction */
    hash_t *pglogs_hashtbl;
};

struct timestamp_lsn_key {
    int32_t timestamp;
    DB_LSN lsn;
    unsigned long long context;
};

#ifdef NEWSI_ASOF_USE_TEMPTABLE
typedef struct pglogs_tmptbl_key {
    db_pgno_t pgno;
    DB_LSN commit_lsn;
    DB_LSN lsn;
} pglogs_tmptbl_key;
typedef struct {
    unsigned char fileid[DB_FILE_ID_LEN];
    struct temp_table *tmptbl;
    struct temp_cursor *tmpcur;
    pthread_mutex_t mtx;
#ifdef NEWSI_DEBUG_POOL
    void *pool;
#endif
} logfile_pglog_hashkey;

typedef struct relinks_tmptbl_key {
    db_pgno_t pgno;
    DB_LSN lsn;
    db_pgno_t inh;
} relinks_tmptbl_key;
typedef logfile_pglog_hashkey logfile_relink_hashkey;
#define LOGFILE_PAGE_KEY_SIZE (DB_FILE_ID_LEN * sizeof(unsigned char))
#else
typedef struct pglogs_logical_key logfile_pglog_hashkey;
typedef struct pglogs_relink_key logfile_relink_hashkey;
#define LOGFILE_PAGE_KEY_SIZE                                                  \
    (DB_FILE_ID_LEN * sizeof(unsigned char) + sizeof(db_pgno_t))
#endif

#define LOGFILE_PGLOG_OFFSET (offsetof(logfile_pglog_hashkey, fileid))
#define LOGFILE_RELINK_OFFSET (offsetof(logfile_relink_hashkey, fileid))

struct logfile_pglogs_entry {
    u_int32_t filenum;
    pthread_mutex_t pglogs_mutex;
    hash_t *pglogs_hashtbl;
    hash_t *relinks_hashtbl;
};

struct checkpoint_list {
    DB_LSN lsn;
    DB_LSN ckp_lsn;
    int32_t timestamp;
    LINKC_T(struct checkpoint_list) lnk;
};

struct tran_tag {
    tranclass_type tranclass;
    DB_TXN *tid;
    u_int32_t logical_lid;

    void *usrptr;
    DB_LSN savelsn;
    struct tran_tag *parent;
    DB_LSN begin_lsn; /* lsn of logical begin */
    DB_LSN startlsn;  /* where log was when we
                         started */

    /*
       snapshot bdb_state->numchildren
       we don't care that much if DB-s are flipping,
       but we don't want to see transient tailing DB-s
       created by schema change or fastinit
     */
    int numchildren;
    /*
       this is index by dbnum;
       right now 0, 1 are meta, among them is also fstblk
       these will never have shadows (shrugs)
     */
    tran_table_shadows_t *tables; /* shadow for tables */

    /* this is a replacement for the genid_bitmap, keep both for now */
    unsigned long long gblcontext;

    unsigned long long logical_tranid;

    /* LSN of the last logical record for this transaction */
    DB_LSN last_logical_lsn;

    DB_LSN last_physical_commit_lsn;

    /* which lsn generated the startgenid */
    DB_LSN snapy_commit_lsn;
    uint32_t snapy_commit_generation;

    DB_LSN last_regop_lsn;

    /* LSN of the the physical commit/abort txn */
    DB_LSN commit_lsn;

    /* lsn when the tran obj was created */
    DB_LSN birth_lsn;

    /* Birth lsn of oldest outstanding logical txn at start time */
    DB_LSN oldest_txn_at_start;

    /* List of outstanding logical txns at start */
    uint64_t *bkfill_txn_list;

    /* Number of outstanding logical txns at start */
    int bkfill_txn_count;

    /* tran obj was created as of we were at a lsn*/
    DB_LSN asof_lsn;
    /* oldest logical ref point of a begin-as-of tran*/
    DB_LSN asof_ref_lsn;

    /* hash table for pglogs */
    hash_t *pglogs_hashtbl;
    /* hash table for relinks */
    hash_t *relinks_hashtbl;
    pthread_mutex_t pglogs_mutex;

    /* hash table to keep track of
       whether we have copied pglogs from the gbl structure for a given page */
    hash_t *asof_hashtbl;

    /* temporary: used in logical abort case */
    hash_t *compensated_records;

    /* anchor in bdb_state->transactions */
    LINKC_T(struct tran_tag) tranlist_lnk;

    /* For non-clustered sql offloading we pass the tran object allocated
     * in the block processor to the sql engine pool.  Then when th sql engine
     * creates shadow indexes it uses its thread id as part of the file name.
     * However the shadow files don't get deleted until commit or abort time
     * on the original block processor thread, by which time the sql engine
     * thread may have been freed and reused for another transation.
     * Get round this by recording the threadid of the thread that creates
     * the transaction and using this in shadow file names. */
    pthread_t threadid;

    /* for recom and snapisol/serial, record in startgenid the context when this
       transaction was started;
       - for recom it is used to differentiate between synthetic genids and
       real(existing) genids
       - for si, this is also used to mask out new updates
     */
    unsigned long long startgenid;

    /* For logical transactions: a logical transaction may have a (one and
       only one) physical transaction in flight.  Latch it here for debugging
       and sanity checking */
    struct tran_tag *physical_tran;
    /* For a physical transaction, chain up to the logical transaction */
    struct tran_tag *logical_tran;

    /* snapshot/serializable support */
    struct bdb_osql_trn *osql;

    /* this is tested in rep.c to see if net needs to flush/wait */
    signed char is_about_to_commit;

    signed char aborted;

    signed char rep_handle_dead; /* must reopen all  db cursors after abort */

    /* set if we are a top level transaction (ie, not a child) */
    signed char master;

    /* Set if we were created from the replication stream */
    signed char reptxn;

    signed char wrote_begin_record;
    signed char committed_begin_record;
    signed char get_schema_lock;
    signed char single_physical_transaction;

    /* log support */
    signed char trak; /* set this to enable tracking */

    signed char is_rowlocks_trans;

    /* if the txn intends to write, this tells us to get write
       locks when we read */
    signed char write_intent;

    /* Open cursors under this transaction. */
    LISTC_T(bdb_cursor_ifn_t) open_cursors;

    /* Committed the child transaction. */
    signed char committed_child;

    /* total shadow rows */
    int shadow_rows;

    /* Set to 1 if we got the bdb lock */
    int got_bdb_lock;

    /* Set to 1 if this is a schema change txn */
    int schema_change_txn;

    /* cache the versions of dta files to catch schema changes and fastinits */
    int table_version_cache_sz;
    unsigned long long *table_version_cache;
    bdb_state_type *parent_state;

    /* Send the master periodic 'acks' after this many physical commits */
    int request_ack;

    int check_shadows;

    int micro_commit;

    /* Rowlocks commit support */
    pool_t *rc_pool;
    DBT **rc_list;
    DB_LOCK *rc_locks;
    u_int32_t rc_max;
    u_int32_t rc_count;

    /* Newsi pglogs queue hash */
    hash_t *pglogs_queue_hash;
};

struct seqnum_t {
    DB_LSN lsn;
    // For master lease
    uint32_t issue_time[2];
    uint32_t lease_ms;
    uint32_t commit_generation;
    uint32_t generation;
};

enum { BDB_SEQNUM_TYPE_LEN = 8 + 2 + 2 + 4 + 12 };

BB_COMPILE_TIME_ASSERT(bdb_seqnum_type,
                       sizeof(struct seqnum_t) == BDB_SEQNUM_TYPE_LEN);

struct filepage_t {
    unsigned int fileid; /* fileid to prefault */
    unsigned int pgno;   /* page number to prefault*/
};

enum { BDB_FILEPAGE_TYPE_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(bdb_filepage_type,
                       sizeof(struct filepage_t) == BDB_FILEPAGE_TYPE_LEN);

/* terminate list w/ index == -1 */
typedef struct {
    unsigned long long context;
    short index;
} cmpcontextlist_type;

struct thread_lock_info_tag;
typedef struct thread_lock_info_tag thread_lock_info_type;

#ifndef __bdb_api_h__
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

#endif

struct bdb_queue_priv;
typedef struct bdb_queue_priv bdb_queue_priv;

struct bdb_cursor_thd_tag;
typedef struct bdb_cursor_thd_tag bdb_cursor_thd_t;

enum bdbcursor_types {
    BDBC_UN = 0,
    BDBC_IX = 1,
    BDBC_DT = 2,
    BDBC_SK = 3,
    BDBC_BL = 4
};

char const *cursortype(int type);

/* track the cursor threading */
struct bdb_cursor_thd_tag {
    int x;
};

struct bdb_cursor_impl_tag {

    /* cursor btree info */
    enum bdbcursor_types type; /* BDBC_IX, BDBC_DT */
    int dbnum;                 /* dbnum for this bdbcursor */
    int idx;                   /* BDBC_IX:ixnum, BDBC_DT:split_dta_num */

    /* transaction */
    bdb_state_type *state;  /* state for */
    cursor_tran_t *curtran; /* all cursors (but comdb2 mode have this */
    tran_type *shadow_tran; /* read committed and snapshot/serializable modes */

    /* cursor position */
    int rrn;                  /* == 2 (don't need this) */
    unsigned long long genid; /* genid of current entry */
    void *data;               /* points inside one of  bdb_berkdb_t if valid */
    int datalen;              /* size of payload */

    void *datacopy;
    void *unpacked_datacopy;

    /* new btree access interface */
    bdb_berkdb_t *rl; /* persistent berkdb */
    bdb_berkdb_t *sd; /* shadow berkdb */

    /* comdb2 mode support */
    DBCPS dbcps; /* serialized cursor */

    /* perfmetrics */
    int nsteps; /* count ops */

    /* read committed/snapshot/serializable mode support */
    tmpcursor_t *skip; /* skip list; don't touch this, use bdb_osql please */
    char
        *lastkey; /* set after a row is consumed from real data (see merging) */
    int lastkeylen;
    int laststripe;
    int lastpage;
    int lastindex;

    /* read committed/snapshot/serializable (maybe we should merge this here, in
     * bdb, not in db) */
    tmpcursor_t *addcur; /* cursors for add and upd data shadows; */
    void *addcur_odh;    /* working area for addcur odh. */
    int addcur_use_odh;

    /* page-order read committed/snapshot/serializable/snapisol */
    tmpcursor_t *pgordervs;

    /* support for deadlock */
    int invalidated; /* mark this if the cursor was unlocked */

    /* page-order flags */
    int pageorder;       /* mark if the cursor is in page-order */
    int discardpages;    /* mark if the pages should be discarded immediately */
    tmptable_t *vs_stab; /* Table of records to skip in the virtual stripe. */
    tmpcursor_t *vs_skip; /* Cursor for vs_stab. */

#if 0
   tmptable_t              *cstripe;      /* Cursor stripe */
   tmpcursor_t             *cscur;        /* Cursor for cstripe */
#endif

    int new_skip;              /* Set to 1 when the vs_skip has a new record. */
    int last_skip;             /* Set to 1 if we've passed the last record. */
    unsigned long long agenid; /* The last addcur genid. */
    int repo_addcur;           /* Set to 1 if we've added to addcur. */
    int threaded;              /* mark if this is this is threaded */

    int upd_shadows_count;

    /* XXX todo */
    bdb_cursor_thd_t *thdinfo; /* track cursor threadinfo */

    /* if pointer */
    struct bdb_cursor_ifn *ifn;

    /* col attributes */
    char *collattr; /* pointer to tailing data, if any */
    int collattr_len;

    /* snapisol may need prescanning the updates to filter out
       older genids added by younger commits */
    int need_prescan;

    int *pagelockflag;

    int max_page_locks;
    int rowlocks;

    struct pglogs_queue_cursor *queue_cursor;

    uint8_t ver;
    uint8_t trak;    /* debug this cursor: set to 1 for verbose */
    uint8_t used_rl; /* set to 1 if rl position was consumed */
    uint8_t used_sd; /* set to 1 if sd position was consumed */
};

struct bdb_cursor_ser_int {
    uint8_t is_valid;
    DBCS dbcs;
};
typedef struct bdb_cursor_ser_int bdb_cursor_ser_int_t;

#include "bdb_api.h"
#include "list.h"

struct deferred_berkdb_option {
    char *attr;
    char *value;
    int ivalue;
    LINKC_T(struct deferred_berkdb_option) lnk;
};

struct bdb_attr_tag {
#define DEF_ATTR(NAME, name, type, dflt, desc) int name;
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    int name;
#include "attr.h"
#undef DEF_ATTR
#undef DEF_ATTR_2

    LISTC_T(struct deferred_berkdb_option) deferred_berkdb_options;
};

typedef int (*BDBFP)(); /*was called FP, but that clashed with dbutil.h - sj */

struct bdb_callback_tag {
    WHOISMASTERFP whoismaster_rtn;
    NODEUPFP nodeup_rtn;
    GETROOMFP getroom_rtn;
    REPFAILFP repfail_rtn;
    BDBAPPSOCKFP appsock_rtn;
    PRINTFP print_rtn;
    BDBELECTSETTINGSFP electsettings_rtn;
    BDBCATCHUPFP catchup_rtn;
    BDBTHREADDUMPFP threaddump_rtn;
    BDBGETFILELWMFP get_file_lwm_rtn;
    BDBSETFILELWMFP set_file_lwm_rtn;
    SCDONEFP scdone_rtn;
    SCABORTFP scabort_rtn;
    UNDOSHADOWFP undoshadow_rtn;
    NODEDOWNFP nodedown_rtn;
    SERIALCHECK serialcheck_rtn;
};

struct waiting_for_lsn {
    DB_LSN lsn;
    int start;
    LINKC_T(struct waiting_for_lsn) lnk;
};

typedef LISTC_T(struct waiting_for_lsn) wait_for_lsn_list;

typedef struct {
    seqnum_type *seqnums; /* 1 per node num */
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_key_t key;
    wait_for_lsn_list **waitlist;
    short *expected_udp_count;
    short *incomming_udp_count;
    short *udp_average_counter;
    int *filenum;

    pool_t *trackpool;
    /* need to do a bit better here... */
    struct averager **time_10seconds;
    struct averager **time_minute;
} seqnum_info_type;

typedef struct {
    int rep_process_message;
    int rep_zerorc;
    int rep_newsite;
    int rep_holdelection;
    int rep_newmaster;
    int rep_dupmaster;
    int rep_isperm;
    int rep_notperm;
    int rep_outdated;
    int rep_other;

    int dummy_adds;
    int commits;
} repstats_type;

struct sockaddr_in;
typedef struct {
    netinfo_type *netinfo;

    char *master_host;
    char *myhost;

    pthread_mutex_t elect_mutex;
    int *appseqnum; /* application level (bdb lib) sequencing */
    pthread_mutex_t appseqnum_lock;
    pthread_mutex_t upgrade_lock; /* ensure only 1 upgrade at a time */
    pthread_mutex_t send_lock;
    repstats_type repstats;
    pthread_mutex_t receive_lock;

    signed char in_rep_process_message;
    signed char disable_watcher;
    signed char in_election; /* true if we are in the middle of an election */
    signed char upgrade_allowed;

    int skipsinceepoch; /* since when have we been incoherent */

    int rep_process_message_start_time;
    int dont_elect_untill_time;

    struct sockaddr_in *udp_addr;
    pthread_t udp_thread;
    int udp_fd;

    int should_reject_timestamp;
    int should_reject;
} repinfo_type;

enum {
    STATE_COHERENT = 0,
    STATE_INCOHERENT = 1,
    STATE_INCOHERENT_SLOW = 2,
    STATE_INCOHERENT_WAIT = 3
};

struct bdb_state_tag;

/* Every time we add a blkseq, if the log file rolled, we add a new
 * entry with the earliest blkseq in the new log.  We maintain this list in
 * bdb_blkseq_insert and in bdb_blkseq_recover (should really call
 * bdb_blkseq_insert
 * in recovery instead). In log deletion code, we walk the list, and disallow
 * deletion
 * for log files where the blkseq is too new. */
struct seen_blkseq {
    u_int32_t logfile;
    int timestamp;
    LINKC_T(struct seen_blkseq) lnk;
};

struct temp_table;

struct bdb_state_tag {
    pthread_attr_t pthread_attr_detach;
    seqnum_info_type *seqnum_info;
    bdb_attr_type *attr;         /* attributes that have defaults */
    bdb_callback_type *callback; /* callback functions */
    DB_ENV *dbenv;               /* transactional environment */
    int read_write;              /* if we opened the db with R/W access */
    repinfo_type *repinfo;       /* replication info */
    signed char numdtafiles;

    /* the berkeley db btrees underlying this "table" */
    DB *dbp_data[MAXDTAFILES][MAXSTRIPE]; /* the data files.  dbp_data[0] is
                                     the primary data file which would contain
                                     the record.  higher files are extra data
                                     aka the blobs.  in blobstripe mode the
                                     blob files are striped too, otherwise
                                     they are not. */
    DB *dbp_ix[MAXIX];                    /* handle for the ixN files */

    pthread_key_t tid_key;

    int numthreads;
    pthread_mutex_t numthreads_lock;

    char *name;         /* name of the comdb */
    char *txndir;       /* name of the transaction directory for log files */
    char *tmpdir;       /* name of directory for tempoarary dbs */
    char *dir;          /* directory the files go in (/bb/data /bb/data2) */
    int lrl;            /* Logical Record Length (0 = variable) */
    short numix;        /* number of indexes */
    short ixlen[MAXIX]; /* size of each index */
    signed char ixdta[MAXIX]; /* does this index contain the dta? */
    signed char
        ixcollattr[MAXIX]; /* does this index contain the column attributes? */
    signed char ixnulls
        [MAXIX]; /*does this index contain any columns that allow nulls?*/
    signed char ixdups[MAXIX];   /* 1 if ix allows dupes, else 0 */
    signed char ixrecnum[MAXIX]; /* 1 if we turned on recnum mode for btrees */

    short keymaxsz; /* size of the keymax buffer */

    /* the helper threads (only valid for a "parent" bdb_state) */
    pthread_t checkpoint_thread;
    pthread_t watcher_thread;
    pthread_t memp_trickle_thread;
    pthread_t logdelete_thread;
    pthread_t lock_detect_thread;
    pthread_t coherency_lease_thread;
    pthread_t master_lease_thread;

    struct bdb_state_tag *parent; /* pointer to our parent */
    short numchildren;
    struct bdb_state_tag *children[MAXTABLES];
    pthread_rwlock_t *bdb_lock;   /* we need this to do safe upgrades.  fetch
                                     operations get a read lock, upgrade requires
                                     a write lock - this way we can close and
                                     re-open databases knowing that there
                                     are no cursors opened on them */
    signed char bdb_lock_desired; /* signal that long running operations like
                                     fast dump should GET OUT! so that we can
                                     upgrade/downgrade */

    void *usr_ptr;

    pthread_t bdb_lock_write_holder;
    thread_lock_info_type *bdb_lock_write_holder_ptr;
    char bdb_lock_write_idstr[80];
    int seed;
    unsigned int last_genid_epoch;
    pthread_mutex_t seed_lock;

    /* One of the BDBTYPE_ constants */
    int bdbtype;

    /* Lite databases have no rrn cache, freerecs files, ix# files */
    int pagesize_override; /* 0, or a power of 2 */

    size_t queue_item_sz; /* size of a queue record in bytes (including
                           * struct bdb_queue_header) */

    /* bit mask of which consumers want to consume new queue items */
    uint32_t active_consumers;

    unsigned long long master_cmpcontext;

    /* stuff for the genid->thread affinity logic */
    int maxthreadid;
    unsigned char stripe_pool[17];
    unsigned char stripe_pool_start;
    pthread_mutex_t last_dta_lk;
    int last_dta;

    /* when did we convert to blobstripe? */
    unsigned long long blobstripe_convert_genid;

    pthread_mutex_t pending_broadcast_lock;

    unsigned long long gblcontext;

    void (*signal_rtoff)(void);

    int checkpoint_start_time;

    hash_t *logical_transactions_hash;
    DB_LSN lwm; /* low watermark for logical transactions */

    /* chain all transactions */
    pthread_mutex_t translist_lk;
    LISTC_T(struct tran_tag) logical_transactions_list;

    /* for queues this points to extra stuff defined in queue.c */
    bdb_queue_priv *qpriv;

    void *temp_list;
    pthread_mutex_t temp_list_lock;
    comdb2_objpool_t temp_table_pool; /* pooled temptables */
    pthread_t priosqlthr;
    int haspriosqlthr;

    int temp_table_id;

    int num_temp_tables;

    DB_MPOOL_STAT *temp_stats;

    pthread_mutex_t id_lock;
    unsigned int id;
    pthread_mutex_t gblcontext_lock;
    pthread_mutex_t children_lock;

    FILE *bdblock_debug_fp;
    pthread_mutex_t bdblock_debug_lock;

    uint8_t version;

    /* access control */
    bdb_access_t *access;

    char *origname; /* name before new.name shenanigans */

    pthread_mutex_t exit_lock;

    signed char have_recnums; /* 1 if ANY index has recnums enabled */

    signed char exiting;
    signed char caught_up; /* if we passed the recovery phase */

    signed char isopen;
    signed char envonly;

    signed char need_to_downgrade_and_lose;

    signed char rep_trace;
    signed char berkdb_rep_startupdone;

    signed char rep_started;

    signed char master_handle;

    signed char sanc_ok;

    signed char ondisk_header; /* boolean: give each record an ondisk header? */
    signed char compress;      /* boolean: compress data? */
    signed char compress_blobs; /*boolean: compress blobs? */

    signed char got_gblcontext;
    signed char need_to_upgrade;

    signed char in_recovery;
    signed char in_bdb_recovery;

    signed char low_headroom_count;

    signed char pending_seqnum_broadcast;
    int *coherent_state;
    uint64_t *master_lease;
    pthread_mutex_t master_lease_lk;

    signed char after_llmeta_init_done;

    pthread_mutex_t coherent_state_lock;

    signed char
        not_coherent; /*master sets this if it knows were not coherent */
    int not_coherent_time;

    uint64_t *last_downgrade_time;

    /* old databases with datacopy don't have odh in index */
    signed char datacopy_odh;

    /* inplace updates setting */
    signed char inplace_updates;

    signed char instant_schema_change;

    signed char rep_handle_dead;

    /* keep this as an int, it's read locklessly */
    int passed_dbenv_open;

    /* These are only used in a parent bdb_state.  This is a linked list of
     * all the thread specific lock info structs.  This is here currently
     * just to make debugging lock issues abit easier. */
    pthread_mutex_t thread_lock_info_list_mutex;
    LISTC_T(thread_lock_info_type) thread_lock_info_list;

    /* cache the version_num for the data; this is used to detect dta changes */
    unsigned long long version_num;

    pthread_cond_t temptable_wait;
#ifdef DEBUG_TEMP_TABLES
    LISTC_T(struct temp_table) busy_temptables;
#endif
    char *recoverylsn;

    int disable_page_order_tablescan;

    pthread_mutex_t *blkseq_lk;
    DB_ENV **blkseq_env;
    DB **blkseq[2];
    time_t blkseq_last_roll_time;
    DB_LSN *blkseq_last_lsn[2];
    LISTC_T(struct seen_blkseq) blkseq_log_list[2];
    uint32_t genid_format;

    /* we keep a per bdb_state copy to enhance locality */
    unsigned int bmaszthresh;
    comdb2bma bma;

    pthread_mutex_t durable_lsn_lk;
    uint16_t *fld_hints;

    int hellofd;
};

/* define our net user types */
enum {
    USER_TYPE_BERKDB_REP = 1,
    USER_TYPE_BERKDB_NEWSEQ = 2,
    USER_TYPE_BERKDB_FILENUM = 3,
    USER_TYPE_TEST = 4,
    USER_TYPE_ADD = 5,
    USER_TYPE_DEL = 6,
    USER_TYPE_DECOM = 7,
    USER_TYPE_ADD_DUMMY = 8,
    USER_TYPE_REPTRC = 9,
    USER_TYPE_RECONNECT = 10,
    USER_TYPE_LSNCMP = 11,
    USER_TYPE_RESYNC = 12,
    USER_TYPE_DOWNGRADEANDLOSE = 13,
    USER_TYPE_INPROCMSG = 14,
    USER_TYPE_COMMITDELAYMORE = 15,
    USER_TYPE_COMMITDELAYNONE = 16,
    USER_TYPE_MASTERCMPCONTEXTLIST = 18,
    USER_TYPE_GETCONTEXT = 19,
    USER_TYPE_HEREISCONTEXT = 20,
    USER_TYPE_TRANSFERMASTER = 21,
    USER_TYPE_GBLCONTEXT = 22,
    USER_TYPE_YOUARENOTCOHERENT = 23,
    USER_TYPE_YOUARECOHERENT = 24,
    USER_TYPE_UDP_ACK,
    USER_TYPE_UDP_PING,
    USER_TYPE_UDP_TIMESTAMP,
    USER_TYPE_UDP_TIMESTAMP_ACK,
    USER_TYPE_UDP_PREFAULT,
    USER_TYPE_TCP_TIMESTAMP,
    USER_TYPE_TCP_TIMESTAMP_ACK,
    USER_TYPE_PING_TIMESTAMP,
    USER_TYPE_PING_TIMESTAMP_ACK,
    USER_TYPE_ANALYZED_TBL,
    USER_TYPE_COHERENCY_LEASE,
    USER_TYPE_PAGE_COMPACT,

    /* by hostname messages */
    USER_TYPE_DECOM_NAME,
    USER_TYPE_ADD_NAME,
    USER_TYPE_DEL_NAME,
    USER_TYPE_TRANSFERMASTER_NAME,
    USER_TYPE_REQ_START_LSN
};

void print(bdb_state_type *bdb_state, char *format, ...);

typedef struct {
    DB_LSN lsn;
    int delta;
} lsn_cmp_type;

enum { BDB_LSN_CMP_TYPE_LEN = 8 + 4 };

BB_COMPILE_TIME_ASSERT(bdb_lsn_cmp_type,
                       sizeof(lsn_cmp_type) == BDB_LSN_CMP_TYPE_LEN);

uint8_t *bdb_lsn_cmp_type_put(const lsn_cmp_type *p_lsn_cmp_type,
                              uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *bdb_lsn_cmp_type_get(lsn_cmp_type *p_lsn_cmp_type,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

typedef struct colease {
    u_int64_t issue_time;
    u_int32_t lease_ms;
    u_int8_t fluff[4];
} colease_t;

enum { COLEASE_TYPE_LEN = 8 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(colease_type_len, sizeof(colease_t) == COLEASE_TYPE_LEN);

const uint8_t *colease_type_get(colease_t *p_colease_type, const uint8_t *p_buf,
                                const uint8_t *p_buf_end);

uint8_t *colease_type_put(const colease_t *p_colease_type, uint8_t *p_buf,
                          uint8_t *p_buf_end);

/* Each data item fragment has this header. */
struct bdb_queue_header {
    /* zero based index of this fragment */
    bbuint16_t fragment_no;

    /* how many fragments make up this record - must be at least 1! */
    bbuint16_t num_fragments;

    /* genid of this item */
    bbuint32_t genid[2];

    /* the total size of this item */
    bbuint32_t total_sz;

    /* the size of this fragment in bytes */
    bbuint32_t fragment_sz;

    /* a bit is set for each consumer that has not yet consumed this record */
    bbuint32_t consumer_mask;

    /* this is a debug feature - prod build just writes zero here.
     * to make sure I reassemble all the fragments correctly. */
    bbuint32_t crc32;

    /* zero for now */
    bbuint32_t reserved;

    /*char data[1];*/
};

enum { QUEUE_HDR_LEN = 2 + 2 + 8 + 4 + 4 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(bdb_queue_header_size,
                       sizeof(struct bdb_queue_header) == QUEUE_HDR_LEN);

enum {
    FETCH_INT_CUR = 0,
    FETCH_INT_NEXT = 1,
    FETCH_INT_PREV = 2,
    FETCH_INT_CUR_LASTDUPE = 3,
    FETCH_INT_CUR_BY_RECNUM = 4
};

extern pthread_key_t bdb_key;

char *bdb_strerror(int error);
char *bdb_trans(const char infile[], char outfile[]);

void *mymalloc(size_t size);
void myfree(void *ptr);
void *myrealloc(void *ptr, size_t size);

void bdb_get_txn_stats(bdb_state_type *bdb_state, int *txn_commits);

int bdb_upgrade(bdb_state_type *bdb_state, uint32_t newgen, int *done);
int bdb_downgrade(bdb_state_type *bdb_state, uint32_t newgen, int *done);
int bdb_downgrade_noelect(bdb_state_type *bdb_state);
int get_seqnum(bdb_state_type *bdb_state, const char *host);
void bdb_set_key(bdb_state_type *bdb_state);

uint64_t subtract_lsn(bdb_state_type *bdb_state, DB_LSN *lsn1, DB_LSN *lsn2);
void get_my_lsn(bdb_state_type *bdb_state, DB_LSN *lsnout);
void rep_all_req(bdb_state_type *bdb_state);
void get_master_lsn(bdb_state_type *bdb_state, DB_LSN *lsnout);
void bdb_print_log_files(bdb_state_type *bdb_state);
char *lsn_to_str(char lsn_str[], DB_LSN *lsn);

int bdb_get_datafile_num_files(bdb_state_type *bdb_state, int dtanum);

/* genid related utilities */
unsigned long long get_genid(bdb_state_type *bdb_state, unsigned int dtafile);
DB *get_dbp_from_genid(bdb_state_type *bdb_state, int dtanum,
                       unsigned long long genid, int *out_dtafile);
unsigned long long set_participant_stripeid(bdb_state_type *bdb_state,
                                            int stripeid,
                                            unsigned long long genid);
unsigned long long set_updateid(bdb_state_type *bdb_state, int updateid,
                                unsigned long long genid);
int get_participant_stripe_from_genid(bdb_state_type *bdb_state,
                                      unsigned long long genid);
int get_updateid_from_genid(bdb_state_type *bdb_state,
                            unsigned long long genid);
int max_participant_stripeid(bdb_state_type *bdb_state);
int max_updateid(bdb_state_type *bdb_state);
unsigned long long get_search_genid(bdb_state_type *bdb_state,
                                    unsigned long long genid);

unsigned long long get_search_genid_rowlocks(unsigned long long genid);

int bdb_inplace_cmp_genids(bdb_state_type *bdb_state, unsigned long long g1,
                           unsigned long long g2);

/* prototype pinched from Berkeley DB internals. */
int __rep_send_message(DB_ENV *, char *, u_int32_t, DB_LSN *, const DBT *,
                       u_int32_t, void *);

/* Error handling utilities */
int bdb_dbcp_close(DBC **dbcp_ptr, int *bdberr, const char *context_str);
void bdb_cursor_error(bdb_state_type *bdb_state, DB_TXN *tid, int rc,
                      int *bdberr, const char *context_str);
void bdb_get_error(bdb_state_type *bdb_state, DB_TXN *tid, int rc,
                   int not_found_rc, int *bdberr, const char *context_str);
void bdb_c_get_error(bdb_state_type *bdb_state, DB_TXN *tid, DBC **dbcp, int rc,
                     int not_found_rc, int *bdberr, const char *context_str);

/* compression wrappers for I/O */
void bdb_maybe_compress_data(bdb_state_type *bdb_state, DBT *data, DBT *data2);
void bdb_maybe_uncompress_data(bdb_state_type *bdb_state, DBT *data,
                               DBT *data2);

int bdb_cget_unpack(bdb_state_type *bdb_state, DBC *dbcp, DBT *key, DBT *data,
                    uint8_t *ver, u_int32_t flags);
int bdb_cget_unpack_blob(bdb_state_type *bdb_state, DBC *dbcp, DBT *key,
                         DBT *data, uint8_t *ver, u_int32_t flags);
int bdb_get_unpack_blob(bdb_state_type *bdb_state, DB *db, DB_TXN *tid,
                        DBT *key, DBT *data, uint8_t *ver, u_int32_t flags);
int bdb_get_unpack(bdb_state_type *bdb_state, DB *db, DB_TXN *tid, DBT *key,
                   DBT *data, uint8_t *ver, u_int32_t flags);
int bdb_put_pack(bdb_state_type *bdb_state, int is_blob, DB *db, DB_TXN *tid,
                 DBT *key, DBT *data, u_int32_t flags);

int bdb_cput_pack(bdb_state_type *bdb_state, int is_blob, DBC *dbcp, DBT *key,
                  DBT *data, u_int32_t flags);

int bdb_put(bdb_state_type *bdb_state, DB *db, DB_TXN *tid, DBT *key, DBT *data,
            u_int32_t flags);

int bdb_cposition(bdb_state_type *bdb_state, DBC *dbcp, DBT *key,
                  u_int32_t flags);

int bdb_update_updateid(bdb_state_type *bdb_state, DBC *dbcp,
                        unsigned long long oldgenid,
                        unsigned long long newgenid);

int bdb_cget(bdb_state_type *bdb_state, DBC *dbcp, DBT *key, DBT *data,
             u_int32_t flags);

void init_odh(bdb_state_type *bdb_state, struct odh *odh, void *rec,
              size_t reclen, int dtanum);

int bdb_pack(bdb_state_type *bdb_state, const struct odh *odh, void *to,
             size_t tolen, void **recptr, uint32_t *recsize, void **freeptr);

int bdb_unpack(bdb_state_type *bdb_state, const void *from, size_t fromlen,
               void *to, size_t tolen, struct odh *odh, void **freeptr);

int bdb_retrieve_updateid(bdb_state_type *bdb_state, const void *from,
                          size_t fromlen);

int ip_updates_enabled_sc(bdb_state_type *bdb_state);
int ip_updates_enabled(bdb_state_type *bdb_state);

/* file.c */
void delete_log_files(bdb_state_type *bdb_state);
void delete_log_files_list(bdb_state_type *bdb_state, char **list);
void delete_log_files_chkpt(bdb_state_type *bdb_state);
int bdb_checkpoint_list_init();
int bdb_checkpoint_list_push(DB_LSN lsn, DB_LSN ckp_lsn, int32_t timestamp);
void bdb_checkpoint_list_get_ckplsn_before_lsn(DB_LSN lsn, DB_LSN *lsnout);

/* rep.c */
int bdb_is_skip(bdb_state_type *bdb_state, int node);

void bdb_set_skip(bdb_state_type *bdb_state, int node);

void bdb_clear_skip(bdb_state_type *bdb_state, int node);

/* remove all nodes from skip list */
void bdb_clear_skip_list(bdb_state_type *bdb_state);

/* tran.c */
int bdb_tran_rep_handle_dead(bdb_state_type *bdb_state);

/* useful debug stuff we've exposed in berkdb */
extern void __bb_dbreg_print_dblist(DB_ENV *dbenv,
                                    void (*prncallback)(void *userptr,
                                                        const char *fmt, ...),
                                    void *userptr);
#if 0
extern void __db_cprint(DB *db);
#endif

void bdb_queue_init_priv(bdb_state_type *bdb_state);

unsigned long long bdb_get_gblcontext(bdb_state_type *bdb_state);

int bdb_apprec(DB_ENV *dbenv, DBT *log_rec, DB_LSN *lsn, db_recops op);

int bdb_rowlock_int(DB_ENV *dbenv, DB_TXN *txn, unsigned long long genid,
                    int exclusive);

int rep_caught_up(bdb_state_type *bdb_state);

void call_for_election(bdb_state_type *bdb_state, const char *func, int line);

int bdb_next_dtafile(bdb_state_type *bdb_state);

int ll_key_add(bdb_state_type *bdb_state, unsigned long long genid,
               tran_type *tran, int ixnum, DBT *dbt_key, DBT *dbt_data);
int ll_dta_add(bdb_state_type *bdb_state, unsigned long long genid, DB *dbp,
               tran_type *tran, int dtafile, int dtastripe, DBT *dbt_key,
               DBT *dbt_data, int flags);

int ll_dta_upd(bdb_state_type *bdb_state, int rrn, unsigned long long oldgenid,
               unsigned long long *newgenid, DB *dbp, tran_type *tran,
               int dtafile, int dtastripe, int participantstripid,
               int use_new_genid, DBT *verify_dta, DBT *dta, DBT *old_dta_out);

int ll_dta_upd_blob(bdb_state_type *bdb_state, int rrn,
                    unsigned long long oldgenid, unsigned long long newgenid,
                    DB *dbp, tran_type *tran, int dtafile,
                    int participantstripid, int use_new_genid, DBT *dta);

int ll_dta_upd_blob_w_opt(bdb_state_type *bdb_state, int rrn,
                          unsigned long long oldgenid,
                          unsigned long long *newgenid, DB *dbp,
                          tran_type *tran, int dtafile, int dtastripe,
                          int participantstripid, int use_new_genid,
                          DBT *verify_dta, DBT *dta, DBT *old_dta_out);

int ll_key_upd(bdb_state_type *bdb_state, tran_type *tran, char *table_name,
               unsigned long long oldgenid, unsigned long long genid, void *key,
               int ixnum, int keylen, void *dta, int dtalen);

int ll_key_del(bdb_state_type *bdb_state, tran_type *tran, int ixnum, void *key,
               int keylen, int rrn, unsigned long long genid, int *payloadsz);

int ll_dta_del(bdb_state_type *bdb_state, tran_type *tran, int rrn,
               unsigned long long genid, DB *dbp, int dtafile, int dtastripe,
               DBT *dta_out);

int ll_dta_upgrade(bdb_state_type *bdb_state, int rrn, unsigned long long genid,
                   DB *dbp, tran_type *tran, int dtafile, int dtastripe,
                   DBT *dta);

int add_snapisol_logging(bdb_state_type *bdb_state);
int phys_key_add(bdb_state_type *bdb_state, tran_type *tran,
                 unsigned long long genid, int ixnum, DBT *dbt_key,
                 DBT *dbt_data);
int phys_dta_add(bdb_state_type *bdb_state, tran_type *tran,
                 unsigned long long genid, DB *dbp, int dtafile, int dtastripe,
                 DBT *dbt_key, DBT *dbt_data);

int get_physical_transaction(bdb_state_type *bdb_state, tran_type *logical_tran,
                             tran_type **outtran, int force_commit);
int phys_dta_upd(bdb_state_type *bdb_state, int rrn,
                 unsigned long long oldgenid, unsigned long long *newgenid,
                 DB *dbp, tran_type *logical_tran, int dtafile, int dtastripe,
                 DBT *verify_dta, DBT *dta);

int phys_key_upd(bdb_state_type *bdb_state, tran_type *tran, char *table_name,
                 unsigned long long oldgenid, unsigned long long genid,
                 void *key, int ix, int keylen, void *dta, int dtalen,
                 int llog_payload_len);

int phys_rowlocks_log_bench_lk(bdb_state_type *bdb_state,
                               tran_type *logical_tran, int op, int arg1,
                               int arg2, void *payload, int paylen);

int phys_key_del(bdb_state_type *bdb_state, tran_type *logical_tran,
                 unsigned long long genid, int ixnum, DBT *key);
int phys_dta_del(bdb_state_type *bdb_state, tran_type *logical_tran, int rrn,
                 unsigned long long genid, DB *dbp, int dtafile, int dtastripe);

int bdb_llog_add_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long genid, int dtafile, int dtastripe);

int bdb_llog_del_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long genid, DBT *dbt_data, int dtafile,
                        int dtastripe);

int bdb_llog_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                        unsigned long long oldgenid,
                        unsigned long long newgenid, int dtafile, int dtastripe,
                        DBT *olddta);

int bdb_llog_add_ix_lk(bdb_state_type *bdb_state, tran_type *tran, int ix,
                       unsigned long long genid, DBT *key, int dtalen);

int bdb_llog_del_ix_lk(bdb_state_type *bdb_state, tran_type *tran, int ixnum,
                       unsigned long long genid, DBT *dbt_key, int payloadsz);

int bdb_llog_upd_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, void *key, int keylen, int ix,
                       int dtalen, unsigned long long oldgenid,
                       unsigned long long newgenid);

int bdb_llog_rowlocks_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                            int arg1, int arg2, DBT *lock1, DBT *lock2,
                            void *payload, int paylen);

int bdb_llog_commit(bdb_state_type *bdb_state, tran_type *tran, int isabort);
int bdb_save_row_int(bdb_state_type *bdb_state_in, DB_TXN *txnid, char table[],
                     unsigned long long genid);

int abort_logical_transaction(bdb_state_type *bdb_state, tran_type *tran,
                              DB_LSN *lsn, int about_to_commit);

int ll_rowlocks_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                      int arg1, int arg2, void *payload, int paylen);

int ll_checkpoint(bdb_state_type *bdb_state, int force);

int bdb_llog_start(bdb_state_type *bdb_state, tran_type *tran, DB_TXN *txn);

int bdb_run_logical_recovery(bdb_state_type *bdb_state, int locks_only);

tran_type *bdb_tran_continue_logical(bdb_state_type *bdb_state,
                                     unsigned long long tranid, int trak,
                                     int *bdberr);

tran_type *bdb_tran_start_logical_forward_roll(bdb_state_type *bdb_state,
                                               unsigned long long tranid,
                                               int trak, int *bdberr);

tran_type *bdb_tran_start_logical(bdb_state_type *bdb_state,
                                  unsigned long long tranid, int trak,
                                  int *bdberr);

tran_type *bdb_tran_start_logical_sc(bdb_state_type *bdb_state,
                                     unsigned long long tranid, int trak,
                                     int *bdberr);

int ll_undo_add_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                      char *table_name, int ixnum, void *key, int keylen,
                      DB_LSN *undolsn);

int ll_undo_add_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, unsigned long long genid,
                       DB_LSN *undolsn, int dtafile, int dtastripe);

int ll_undo_del_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                      char *table_name, unsigned long long genid, int ixnum,
                      DB_LSN *undolsn, void *key, int keylen, void *dta,
                      int dtalen);

int ll_undo_del_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, unsigned long long genid,
                       DB_LSN *undolsn, int dtafile, int dtastripe, void *dta,
                       int dtalen);

int ll_undo_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                       char *table_name, unsigned long long oldgenid,
                       unsigned long long newgenid, void *olddta,
                       int olddta_len, int dtafile, int dtastripe,
                       DB_LSN *undolsn);

int ll_undo_upd_ix_lk(bdb_state_type *bdb_state, tran_type *tran,
                      char *table_name, int ixnum, void *key, int keylen,
                      void *dta, int dtalen, DB_LSN *undolsn, void *diff,
                      int difflen, int suffix);

int ll_undo_inplace_upd_dta_lk(bdb_state_type *bdb_state, tran_type *tran,
                               char *table_name, unsigned long long oldgenid,
                               unsigned long long newgenid, void *olddta,
                               int olddta_len, int dtafile, int dtastripe,
                               DB_LSN *undolsn);
bdb_state_type *bdb_get_table_by_name(bdb_state_type *bdb_state, char *table);

int bdb_llog_comprec(bdb_state_type *bdb_state, tran_type *tran, DB_LSN *lsn);

DB *bdb_temp_table_gettbl(struct temp_table *tbl);

void timeval_to_timespec(struct timeval *tv, struct timespec *ts);
void add_millisecs_to_timespec(struct timespec *orig, int millisecs);
int setup_waittime(struct timespec *waittime, int waitms);

int bdb_keycontainsgenid(bdb_state_type *bdb_state, int ixnum);
int bdb_maybe_use_genid_for_key(bdb_state_type *bdb_state, DBT *p_dbt_key, void *ixdta, int ixnum, unsigned long long genid, int isnull, void **ppKeyMaxBuf);

void send_filenum_to_all(bdb_state_type *bdb_state, int filenum, int nodelay);

int bdb_get_file_lwm(bdb_state_type *bdb_state, tran_type *tran, DB_LSN *lsn,
                     int *bdberr);
int bdb_set_file_lwm(bdb_state_type *bdb_state, tran_type *tran, DB_LSN *lsn,
                     int *bdberr);
int bdb_delete_file_lwm(bdb_state_type *bdb_state, tran_type *tran,
                        int *bdberr);

int bdb_update_startlsn(struct tran_tag *intran, DB_LSN *firstlsn);

int bdb_release_ltran_locks(bdb_state_type *bdb_state, struct tran_tag *ltran,
                            int lockerid);

/* Update the startlsn- you must be holding the translist_lk while calling this
 */
int bdb_update_startlsn_lk(bdb_state_type *bdb_state, struct tran_tag *intran,
                           DB_LSN *firstlsn);

tran_type *bdb_tran_begin_logical_int(bdb_state_type *bdb_state,
                                      unsigned long long tranid, int trak,
                                      int *bdberr);

tran_type *bdb_tran_begin_logical_norowlocks_int(bdb_state_type *bdb_state,
                                                 unsigned long long tranid,
                                                 int trak, int *bdberr);

tran_type *bdb_tran_begin_notxn_int(bdb_state_type *bdb_state,
                                    tran_type *parent, int *bdberr);

tran_type *bdb_tran_begin_phys(bdb_state_type *bdb_state,
                               tran_type *logical_tran);

int bdb_tran_commit_phys(bdb_state_type *bdb_state, tran_type *tran);
int bdb_tran_commit_phys_getlsn(bdb_state_type *bdb_state, tran_type *tran,
                                DB_LSN *lsn);
int bdb_tran_abort_phys(bdb_state_type *bdb_state, tran_type *tran);
int bdb_tran_abort_phys_retry(bdb_state_type *bdb_state, tran_type *tran);

/* for either logical or berk txns */
int bdb_tran_abort_int(bdb_state_type *bdb_state, tran_type *tran, int *bdberr,
                       void *blkseq, int blklen, void *seqkey, int seqkeylen,
                       int *priority);

int ll_dta_del(bdb_state_type *bdb_state, tran_type *tran, int rrn,
               unsigned long long genid, DB *dbp, int dtafile, int dtastripe,
               DBT *dta_out);

int form_stripelock_keyname(bdb_state_type *bdb_state, int stripe,
                            char *keynamebuf, DBT *dbt_out);
int form_rowlock_keyname(bdb_state_type *bdb_state, int ixnum,
                         unsigned long long genid, char *keynamebuf,
                         DBT *dbt_out);
int form_keylock_keyname(bdb_state_type *bdb_state, int ixnum, void *key,
                         int keylen, char *keynamebuf, DBT *dbt_out);

void set_gblcontext(bdb_state_type *bdb_state, unsigned long long gblcontext);
unsigned long long get_gblcontext(bdb_state_type *bdb_state);

void bdb_bdblock_debug_init(bdb_state_type *bdb_state);

/* berkdb creator function */
bdb_berkdb_t *bdb_berkdb_open(bdb_cursor_impl_t *cur, int type, int maxdata,
                              int maxkey, int *bdberr);

void bdb_update_ltran_lsns(bdb_state_type *bdb_state, DB_LSN regop_lsn,
                           const void *args, unsigned int rectype);

int update_shadows_beforecommit(bdb_state_type *bdb_state, DB_LSN *lsn,
                                unsigned long long *commit_genid,
                                int is_master);

int timestamp_lsn_keycmp(void *_, int key1len, const void *key1, int key2len,
                         const void *key2);

/**
 * Return a cursor to a shadow file, either index or data
 * "create" indicate if the shadow file should be created (1)
 * or not (0) if it does not exist
 * Returned cursor is cached for further usage
 *
 */
tmpcursor_t *bdb_tran_open_shadow(bdb_state_type *bdb_state, int dbnum,
                                  tran_type *shadow_tran, int idx, int type,
                                  int create, int *bdberr);

/**
 * Create the underlying shadow table as above, but do not return
 * a cursor.
 *
 */

void bdb_tran_open_shadow_nocursor(bdb_state_type *bdb_state, int dbnum,
                                   tran_type *shadow_tran, int idx, int type,
                                   int *bdberr);

/**
 * Creates a shadow btree if needed (if there are backfill logs for it)
 * Returns the shadow btree if created/existing with the same semantics
 * as bdb_tran_open_shadow
 *
 */
tmpcursor_t *bdb_osql_open_backfilled_shadows(bdb_cursor_impl_t *cur,
                                              struct bdb_osql_trn *trn,
                                              int type, int *bdberr);

/**
 * I need a temp table that does not jump at start after reaching
 * the end of file
 *
 */
int bdb_temp_table_next_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cursor, int *bdberr);
int bdb_temp_table_prev_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cursor, int *bdberr);

bdb_state_type *bdb_get_table_by_name_dbnum(bdb_state_type *bdb_state,
                                            char *table, int *dbnum);

int bdb_get_lsn_lwm(bdb_state_type *bdb_state, DB_LSN *lsnout);

void *bdb_cursor_dbcp(bdb_cursor_impl_t *cur);

extern int gbl_temptable_pool_capacity;
hash_t *bdb_temp_table_histhash_init(void);
int bdb_temp_table_create_pool_wrapper(void **tblp, void *bdb_state_arg);
int bdb_temp_table_destroy_pool_wrapper(void *tbl, void *bdb_state_arg);
int bdb_temp_table_move(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int how, int *bdberr);
int bdb_temp_table_keysize(struct temp_cursor *cursor);
int bdb_temp_table_datasize(struct temp_cursor *cursor);
void *bdb_temp_table_key(struct temp_cursor *cursor);
void *bdb_temp_table_data(struct temp_cursor *cursor);
int bdb_temp_table_stat(bdb_state_type *bdb_state, DB_MPOOL_STAT **gspp);

int bdb_get_active_logical_transaction_lsns(bdb_state_type *bdb_state,
                                            DB_LSN **lsnout, int *numlsns,
                                            int *bdberr,
                                            tran_type *shadow_tran);

unsigned long long get_lowest_genid_for_datafile(int file);

int bdb_get_lid_from_cursortran(cursor_tran_t *curtran);

DBC *get_cursor_for_cursortran_flags(cursor_tran_t *curtran, DB *db,
                                     u_int32_t flags, int *bdberr);

int bdb_bdblock_debug_enabled(void);

int bdb_reconstruct_add(bdb_state_type *state, DB_LSN *startlsn, void *key,
                        int keylen, void *data, int datalen, int *p_outlen);
int bdb_reconstruct_delete(bdb_state_type *state, DB_LSN *startlsn, int *page,
                           int *index, void *key, int keylen, void *data,
                           int datalen, int *outdatalen);

int bdb_write_preamble(bdb_state_type *bdb_state, int *bdberr);

int bdb_reconstruct_key_update(bdb_state_type *bdb_state, DB_LSN *startlsn,
                               void **diff, int *offset, int *difflen);

int bdb_reconstruct_inplace_update(bdb_state_type *bdb_state, DB_LSN *startlsn,
                                   void *allcd, int allcd_sz, int *offset,
                                   int *outlen, int *outpage, int *outidx);

unsigned long long get_id(bdb_state_type *bdb_state);

int bdb_get_active_stripe_int(bdb_state_type *bdb_state);
void bdb_dump_active_locks(bdb_state_type *bdb_state, FILE *out);
void bdb_dump_cursors(bdb_state_type *bdb_state, FILE *out);

/* All flavors of rowlocks */
int bdb_lock_ix_value_write(bdb_state_type *bdb_state, tran_type *tran, int idx,
                            DBT *dbt_key, DB_LOCK *lk, DBT *lkname,
                            int trylock);
int bdb_lock_row_write_getlock(bdb_state_type *bdb_state, tran_type *tran,
                               int idx, unsigned long long genid, DB_LOCK *dblk,
                               DBT *lkname, int trylock);

int bdb_lock_row_write_getlock_fromlid(bdb_state_type *bdb_state, int lid,
                                       int idx, unsigned long long genid,
                                       DB_LOCK *lk, DBT *lkname);

/* Minmax lock routines - these are locks on start/end of a file */
int bdb_lock_minmax(bdb_state_type *bdb_state, int ixnum, int stripe,
                    int minmax, int how, DB_LOCK *dblk, DBT *lkname, int lid,
                    int trylock);

/* Get-lock protocol functions.  These expect the cursor to be positioned. */

/* rowlocks have the fileid of the row baked into them */
int bdb_get_row_lock(bdb_state_type *bdb_state, int rowlock_lid, int idx,
                     DBC *dbcp, unsigned long long genid, DB_LOCK *rlk,
                     DBT *lkname, int how);

int bdb_get_row_lock_pfunc(bdb_state_type *bdb_state, int rowlock_lid, int idx,
                           DBC *dbcp, int (*pfunc)(void *), void *arg,
                           unsigned long long genid, DB_LOCK *rlk, DBT *lkname,
                           int how);

int bdb_get_row_lock_minmaxlk(bdb_state_type *bdb_state, int rowlock_lid,
                              DBC *dbcp, int idx, int stripe, int minmax,
                              DB_LOCK *rlk, DBT *lkname, int how);

int bdb_get_row_lock_minmaxlk_pfunc(bdb_state_type *bdb_state, int rowlock_lid,
                                    DBC *dbcp, int (*pfunc)(void *), void *arg,
                                    int idx, int stripe, int minmax,
                                    DB_LOCK *rlk, DBT *lkname, int how);

int bdb_release_lock(bdb_state_type *bdb_state, DB_LOCK *rowlock);

int bdb_release_row_lock(bdb_state_type *bdb_state, DB_LOCK *rowlock);

int bdb_describe_lock_dbt(DB_ENV *dbenv, DBT *dbtlk, char *out, int outlen);

int bdb_describe_lock(DB_ENV *dbenv, DB_LOCK *lk, char *out, int outlen);

int bdb_lock_row_fromlid(bdb_state_type *bdb_state, int lid, int idx,
                         unsigned long long genid, int how, DB_LOCK *dblk,
                         DBT *lkname, int trylock);

int bdb_lock_row_fromlid_int(bdb_state_type *bdb_state, int lid, int idx,
                             unsigned long long genid, int how, DB_LOCK *dblk,
                             DBT *lkname, int trylock, int flags);

/* we use this structure to create a dummy cursor to be used for all
 * non-transactional cursors. it is defined below */
struct cursor_tran {
    unsigned int lockerid;
    int id; /* debugging */
};

void bdb_curtran_cursor_opened(cursor_tran_t *curtran);
void bdb_curtran_cursor_closed(cursor_tran_t *curtran);
int bdb_curtran_freed(cursor_tran_t *curtran);

extern int __db_count_cursors(DB *db);
extern int __dbenv_count_cursors_dbenv(DB_ENV *dbenv);
int bdb_dump_log(DB_ENV *dbenv, DB_LSN *startlsn);
int release_locks_for_logical_transaction_object(bdb_state_type *bdb_state,
                                                 tran_type *tran, int *bdberr);

extern int bdb_reconstruct_update(bdb_state_type *bdb_state, DB_LSN *startlsn,
                                  int *page, int *index, void *key, int keylen,
                                  void *data, int datalen);

int tran_allocate_rlptr(tran_type *tran, DBT **ptr, DB_LOCK **lptr);
int tran_deallocate_pop(tran_type *tran, int count);
int tran_reset_rowlist(tran_type *tran);

void unpack_index_odh(bdb_state_type *bdb_state, DBT *data, void *foundgenid,
                      void *dta, int dtalen, int *reqdtalen, uint8_t *ver);

int bdb_reopen_inline(bdb_state_type *);
void bdb_setmaster(bdb_state_type *bdb_state, char *host);
int __db_check_all_btree_cursors(DB *dbp, db_pgno_t pgno);
void __db_err(const DB_ENV *dbenv, const char *fmt, ...);

void call_for_election_and_lose(bdb_state_type *bdb_state, const char *func,
                                int line);

extern int gbl_sql_tranlevel_default;
extern int gbl_sql_tranlevel_preserved;

extern int gbl_rowlocks;
extern int gbl_new_snapisol;
extern int gbl_new_snapisol_asof;
extern int gbl_new_snapisol_logging;
extern int gbl_early;
extern int gbl_udp;
extern int gbl_prefault_udp;
extern int gbl_verify_all_pools;

typedef struct udppf_rq {
    bdb_state_type *bdb_state;
    unsigned int fileid;
    unsigned int pgno;
} udppf_rq_t;

void start_udp_reader(bdb_state_type *bdb_state);
void *udpbackup_and_autoanalyze_thd(void *arg);

int do_ack(bdb_state_type *bdb_state, DB_LSN permlsn, uint32_t generation);
void berkdb_receive_rtn(void *ack_handle, void *usr_ptr, char *from_host,
                        int usertype, void *dta, int dtalen, uint8_t is_tcp);
void berkdb_receive_msg(void *ack_handle, void *usr_ptr, char *from_host,
                        int usertype, void *dta, int dtalen, uint8_t is_tcp);
void receive_coherency_lease(void *ack_handle, void *usr_ptr, char *from_host,
                             int usertype, void *dta, int dtalen,
                             uint8_t is_tcp);
void receive_start_lsn_request(void *ack_handle, void *usr_ptr, char *from_host,
                             int usertype, void *dta, int dtalen,
                             uint8_t is_tcp);
uint8_t *rep_berkdb_seqnum_type_put(const seqnum_type *p_seqnum_type,
                                    uint8_t *p_buf, const uint8_t *p_buf_end);
uint8_t *rep_udp_filepage_type_put(const filepage_type *p_filepage_type,
                                   uint8_t *p_buf, const uint8_t *p_buf_end);
void poke_updateid(void *buf, int updateid);

void bdb_genid_sanity_check(bdb_state_type *bdb_state, unsigned long long genid,
                            int stripe);

/* Request on the wire */
typedef struct pgcomp_snd {
    int32_t id;
    uint32_t size;
    /* payload */
} pgcomp_snd_t;

enum { BDB_PGCOMP_SND_TYPE_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(bdb_pgcomp_snd_type,
                       sizeof(pgcomp_snd_t) == BDB_PGCOMP_SND_TYPE_LEN);

const uint8_t *pgcomp_snd_type_get(pgcomp_snd_t *p_snd, const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);
uint8_t *pgcomp_snd_type_put(const pgcomp_snd_t *p_snd, uint8_t *p_buf,
                             const uint8_t *p_buf_end, const void *data);

/* Page compact request on receiver's end */
typedef struct pgcomp_rcv {
    bdb_state_type *bdb_state;
    int32_t id;
    uint32_t size;
    char data[1];
} pgcomp_rcv_t;

int enqueue_pg_compact_work(bdb_state_type *bdb_state, int32_t fileid,
                            uint32_t size, const void *data);

void add_dummy(bdb_state_type *);
int bdb_add_dummy_llmeta(void);
int bdb_have_ipu(bdb_state_type *bdb_state);

typedef struct ack_info_t ack_info;
void handle_tcp_timestamp(bdb_state_type *, ack_info *, char *to);
void handle_tcp_timestamp_ack(bdb_state_type *, ack_info *);
void handle_ping_timestamp(bdb_state_type *, ack_info *, char *to);

unsigned long long bdb_logical_tranid(void *tran);

int bdb_lite_list_records(bdb_state_type *bdb_state,
                          int (*userfunc)(bdb_state_type *bdb_state, void *key,
                                          int keylen, void *data, int datalen,
                                          int *bdberr),
                          int *bdberr);

int bdb_osql_cache_table_versions(bdb_state_type *bdb_state, tran_type *tran,
                                  int trak, int *bdberr);
int bdb_temp_table_destroy_lru(struct temp_table *tbl,
                               bdb_state_type *bdb_state, int *last,
                               int *bdberr);
void wait_for_sc_to_stop(void);
void allow_sc_to_run(void);
int is_table_in_schema_change(const char *tbname, tran_type *tran);

void bdb_temp_table_init(bdb_state_type *bdb_state);

int is_incoherent(bdb_state_type *bdb_state, const char *host);

int berkdb_start_logical(DB_ENV *dbenv, void *state, uint64_t ltranid,
                         DB_LSN *lsn);
int berkdb_commit_logical(DB_ENV *dbenv, void *state, uint64_t ltranid,
                          DB_LSN *lsn);

void send_coherency_leases(bdb_state_type *bdb_state, int lease_time,
                           int *do_add);

int has_low_headroom(const char *path, int threshold, int debug);

const char *deadlock_policy_str(u_int32_t policy);
int deadlock_policy_max();

char *coherent_state_to_str(int state);

#endif /* __bdb_int_h__ */
