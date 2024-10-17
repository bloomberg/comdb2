/*
   Copyright 2015, 2021 Bloomberg Finance L.P.

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

#ifndef INCLUDED_COMDB2_H
#define INCLUDED_COMDB2_H

/* MOVE THESE TO COMDB2_API.H */

#define SQLHERR_ROLLBACKTOOLARGE (-108)
#define SQLHERR_ROLLBACK_TOOOLD (-109)
#define SQLHERR_ROLLBACK_NOLOG (-110)
#define MAXVER 255

#define SP_FILE_NAME "stored_procedures.sp"
#define SP_VERS_FILE_NAME "vers_stored_procedures.sp"
#define TIMEPART_FILE_NAME "time_partitions.tp"
#define REPOP_QDB_FMT "%s/%s.%d.queuedb" /* /dir/dbname.num.ext */

#define DEFAULT_USER "default"
#define DEFAULT_PASSWORD ""

#define COMDB2_STATIC_TABLE "_comdb2_static_table"

enum { IOTIMEOUTMS = 10000 };

struct dbtable;
struct consumer;
struct thr_handle;
struct reqlogger;
struct thdpool;
struct schema_change_type;
struct rootpage;

typedef long long tranid_t;

#include <sys/types.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <bb_stdint.h>
#include <sbuf2.h>
#include <netinet/in.h>

#include <flibc.h>
#include <endian_core.h>
#include <epochlib.h>
#include <fsnapf.h>
#include <plhash_glue.h>

#include <list.h>
#include <queue.h>
#include <compile_time_assert.h>
#include <history.h>
#include <comdb2_dbinfo.h>
#include <comdb2_trn_intrl.h>
#include <sqlthdpool.h>
#include <prefault.h>
#include <quantize.h>
#include <dlmalloc.h>

#include "sqlinterfaces.h"
#include "errstat.h"
#include "comdb2_rcodes.h"
#include "repl_wait.h"
#include "types.h"
#include "thread_util.h"
#include "request_stats.h"
#include "bdb_osqltrn.h"
#include "thdpool.h"
#include "thrman.h"
#include "comdb2uuid.h"
#include "machclass.h"
#include "shard_range.h"
#include "tunables.h"
#include "comdb2_plugin.h"

#ifndef LUASP
#include <mem_uncategorized.h>
#include <mem_override.h>
#endif

#include <trigger.h>
#include <cdb2_constants.h>
#include <schema_lk.h>
#include "perf.h"
#include "constraints.h"
#include "osqlrpltypes.h"
#include "macc_glue.h"
#include "api_history.h"

/* buffer offset, given base ptr & right ptr */
#define BUFOFF(base, right) ((int)(((char *)right) - ((char *)base)))

/* we will delete at most this many per run of purge_old_blkseq */
#define MAXBLKSEQ_PURGE (5 * 1024)

#define DEC_ROUND_NONE (-1)

enum AUXDB_TYPES {
    AUXDB_NONE = 0
    /* 1 used to be AUXDB_BLKSEQ, but it has been purged */
    ,
    AUXDB_META = 2,
    AUXDB_FSTBLK = 3
};

/* This is thenumber of bytes taken up by the null bitmap in the wire protocol,
 * which traditionally is fixed at 32 bytes (enough for 256 columns). */
enum { NULLBMPWIRELENGTH = 32 };

enum {
    DONT_FORM_KEYS = 0 /*original comdbg requests and '2'-type requests
                         supporeted.  2-requests don't form keys */

    ,
    CLIENT_FORMS_KEYS = 1 /*convert client keys in requests to ondisk format*/
    ,
    SERVER_FORMS_KEYS = 2 /*ignore any keys in client requests: always form
                            keys from data*/
};

#define MAXNULLBITS (MAXCOLUMNS / 8)

enum SYNC_FLAGS {
    REP_SYNC_FULL = 0,   /* all nodes are sync'd before ack */
    REP_SYNC_SOURCE = 1, /* source node only is synchronized before ack */
    REP_SYNC_NONE = 2,   /* run asynchronously */
    REP_SYNC_ROOM = 3,   /* sync to nodes in my machine room */
    REP_SYNC_N = 4       /* wait for N machines */
};

enum OPCODES {
    OP_DBINFO = 0 /*rmtdb info req*/
    ,
    OP_FIND = 4 /*find*/
    ,
    OP_NEXT = 5 /*next*/
    ,
    OP_JSTNX = 11 /*jstnx*/
    ,
    OP_JSTFND = 13 /*jstfnd*/
    ,
    OP_FNDRRN = 15,
    OP_PREV = 29 /*prev*/
    ,
    OP_NUMRRN = 30 /*number rrns*/
    ,
    OP_HIGHRRN = 33 /*highest rrn*/
    ,
    OP_JSTPREV = 37 /*jstprev*/
    ,
    OP_STORED = 41 /*stored proc*/
    ,
    OP_FIND2 = 45 /*new find*/
    ,
    OP_NEXT2 = 46 /*new next*/
    ,
    OP_PREV2 = 47 /*new prev*/
    ,
    OP_JFND2 = 48 /*new just find*/
    ,
    OP_JNXT2 = 49 /*new just next*/
    ,
    OP_JPRV2 = 50 /*new just prev*/
    ,
    OP_RNGEXT2 = 51 /*new range extract (extended limits) */
    ,
    OP_RNGEXTP2 = 52 /*new range extract previous (extended limits) */
    ,
    OP_FNDKLESS = 53,
    OP_JFNDKLESS = 54,
    OP_FORMKEY = 55,
    OP_FNDNXTKLESS = 56,
    OP_FNDPRVKLESS = 57,
    OP_JFNDNXTKLESS = 58,
    OP_JFNDPRVKLESS = 59,
    OP_RNGEXTTAG = 60,
    OP_RNGEXTTAGP = 61,
    OP_FNDRRNX = 62,
    OP_RNGEXTTAGTZ = 63,
    OP_RNGEXTTAGPTZ = 64,
    OP_MSG_TRAP = 99 /*process message trap*/
    ,
    OP_BLOCK = 100,
    OP_FWD_BLOCK = 101 /*forwarded block operation*/
    ,
    OP_DBINFO2 = 102,
    OP_DESCRIBE = 103,
    OP_NEWRNGEX = 104,
    OP_SQL = 105 /* all sql calls set this */
    ,
    OP_REBUILD = 106 /* dummy code for online rebuilds */
    ,
    OP_LONGBLOCK = 107 /* long block transaction opcode */
    ,
    OP_FWD_LBLOCK = 108 /* long block transaction forward opcode */
    ,
    OP_BLOBASK = 109 /* ask for a blob fragment */
    ,
    OP_CLEARTABLE = 110,
    OP_COUNTTABLE = 111
    /* marshalled find requests */
    ,
    OP_ANALYZE = 112,
    OP_RMTFIND = 113,
    OP_RMTFINDLASTDUP = 114,
    OP_RMTFINDNEXT = 115,
    OP_RMTFINDPREV = 116,
    OP_RMTFINDRRN = 117,
    OP_DESCRIBEKEYS = 119 /* describe keys */
    ,
    OP_GETKEYNAMES = 120 /* get key names */
    ,
    OP_FASTINIT = 121,
    OP_PROX_CONFIG = 122

    ,
    OP_TRAN_COMMIT = 123,
    OP_TRAN_ABORT = 124,
    OP_TRAN_FINALIZE = 125,
    OP_CHECK_TRANS = 127,
    OP_NOTCOHERENT = 128,
    OP_NOTCOHERENT2 = 129,
    OP_MAKE_NODE_INCOHERENT = 130,
    OP_CLIENT_STATS = 131,
    OP_FWD_BLOCK_LE = 132,
    OP_UPGRADE = 133 /* dummy code for online upgrade */

    ,
    OP_SORESE = 134 /* no blk buffers here */
    ,

    MAXTYPCNT = 134

    ,
    OP_DEBUG = 200 /* for debugging (unused?) */
};

/* if you add a new blockop, please update toblock_init() and breq2a()
 * in toblock.c */
enum BLOCK_OPS {
    BLOCK_ADDSL = 110,
    BLOCK_ADDSEC = 111,
    BLOCK_SECAFPRI = 112,
    BLOCK_ADNOD = 113,
    BLOCK_DELSC = 114,
    BLOCK_DELSEC = 115,
    BLOCK_DELNOD = 116,
    BLOCK_UPVRRN = 117,
    BLOCK2_ADDDTA = 130,
    BLOCK2_ADDKEY = 131,
    BLOCK2_DELDTA = 132,
    BLOCK2_DELKEY = 133,
    BLOCK2_UPDATE = 134,
    BLOCK2_ADDKL = 135,
    BLOCK2_DELKL = 136,
    BLOCK2_UPDKL = 137,
    BLOCK2_ADDKL_POS = 138,
    BLOCK2_UPDKL_POS = 139,
    BLOCK_DEBUG = 777,
    BLOCK_SEQ = 800,
    BLOCK_USE = 801,
    BLOCK2_USE = 802,
    BLOCK2_SEQ = 803,
    BLOCK2_QBLOB = 804,
    BLOCK2_RNGDELKL = 805,
    BLOCK_SETFLAGS = 806,
    BLOCK2_CUSTOM = 807,
    BLOCK2_QADD = 808,
    BLOCK2_QCONSUME = 809,
    BLOCK2_TZ = 810,
    BLOCK2_SQL = 811, /* obsolete */
    BLOCK2_DELOLDER = 812,
    BLOCK2_TRAN = 813,
    BLOCK2_MODNUM = 814,
    BLOCK2_SOCK_SQL = 815,
    BLOCK2_SCSMSK = 816,
    BLOCK2_RECOM = 817,
    BLOCK2_UPDBYKEY = 818,
    BLOCK2_SERIAL = 819,
    BLOCK2_SQL_PARAMS = 820, /* obsolete */
    BLOCK2_DBGLOG_COOKIE = 821,
    BLOCK2_PRAGMA = 822,
    BLOCK2_SNAPISOL = 823,
    BLOCK2_SEQV2 = 824,
    BLOCK2_UPTBL = 825,
    BLOCK_MAXOPCODE

    /* Used for some condensed blockop stats; this should be the number of
     * opcodes that there actually really are. */
    ,
    NUM_BLOCKOP_OPCODES = 45
};

enum DEBUGREQ { DEBUG_METADB_PUT = 1 };

enum RCODES {
    RC_OK = 0,                 /* SUCCESS */
    ERR_VERIFY = 4,            /* failed verify on updwver */
    ERR_CORRUPT = 8,           /* CORRUPT INDEX */
    ERR_BADRRN = 9,            /* findbyrrn on bad rrn  */
    ERR_ACCESS = 10,           /* access denied  */
    ERR_DTA_FAILED = 11,       /* failed operation on data  */
    ERR_INTERNAL = 177,        /* internal logic error. */
    ERR_REJECTED = 188,        /* request rejected */
    RC_INTERNAL_FORWARD = 193, /* forwarded block request */
    ERR_FAILED_FORWARD = 194,  /* block update failed to send to remote */
    ERR_READONLY = 195,        /* database is currently read-only */
    ERR_NOMASTER = 1000,       /* database has no master, it is readonly */
    ERR_NESTED = 1001,         /* this is not master, returns actual master */
    ERR_RMTDB_NESTED = 198,    /* reserved for use by rmtdb/prox2 */
    ERR_BADREQ = 199,          /* bad request parameters */
    ERR_TRAN_TOO_BIG = 208,    /* transaction exceeded size limit */
    ERR_TXN_EXCEEDED_TIME_LIMIT = 209,
    ERR_BLOCK_FAILED = 220,  /* block update failed */
    ERR_NOTSERIAL = 230,     /* transaction not serializable */
    ERR_SC = 240,            /* schemachange failed */
    RC_INTERNAL_RETRY = 300, /* need to retry comdb upper level request */
    ERR_CONVERT_DTA = 301,
    ERR_CONVERT_IX = 301,
    ERR_KEYFORM_UNIMP = 303,
    RC_TRAN_TOO_COMPLEX = 304, /* too many rrns allocated per trans */
    RC_TRAN_CLIENT_RETRY = 305,
    ERR_BLOB_TOO_LARGE = 306, /* a blob exceeded MAXBLOBLENGTH */
    ERR_BUF_TOO_SMALL = 307,  /* buffer provided too small to fit data  */
    ERR_NO_BUFFER = 308,      /* can't get fstsnd buffer  */
    ERR_JAVASP_ABORT = 309,   /* stored procedure ordered this trans aborted */
    ERR_NO_SUCH_TABLE = 310,  /* operation tried to use non-existant table */
    ERR_CALLBACK = 311,       /* operation failed due to errors in callback */
    ERR_TRAN_FAILED = 312,    /* could not start of finish transaction */
    ERR_CONSTR = 313,         /* could not complete the operation because of
                                 constraints in the table */
    ERR_SC_COMMIT = 314,      /* schema change in its final stages;
                                 proxy should retry */
    ERR_CONFIG_FAILED = 316,
    ERR_NO_RECORDS_FOUND = 317,
    ERR_NULL_CONSTRAINT = 318,
    ERR_VERIFY_PI = 319,
    ERR_CHECK_CONSTRAINT = 320,
    ERR_INDEX_CONFLICT = 330,
    ERR_UNCOMMITTABLE_TXN = 404, /* txn is uncommittable, returns ERR_VERIFY
                                   rather than retry */
    ERR_DIST_ABORT = 430,       /* Prepared txn has been aborted */
    ERR_QUERY_REJECTED = 451,
    ERR_INCOHERENT = 996, /* prox2 understands it should retry another
                             node for 996 */
    ERR_SQL_PREPARE = 1003,
    ERR_NO_AUXDB = 2000,    /* requested auxiliary database not available */
    ERR_SQL_PREP = 2001,    /* block sql error in sqlite3_prepare */
    ERR_LIMIT = 2002,       /* sql request exceeds max cost */
    ERR_NOT_DURABLE = 2003, /* commit didn't make it to a majority */
    ERR_RECOVER_DEADLOCK = 2004
};

#define IS_BAD_IX_FND_RCODE(rc)                                                \
    (((rc) < IX_OK || (rc) > IX_PASTEOF) &&                                    \
     ((rc) < IX_FNDNOCONV || (rc) > IX_PASTEOFNOCONV) && ((rc) != IX_EMPTY))
#include "ix_return_codes.h"

enum COMDB2_TAIL_REPLY_FLAGS {
    TAIL_EMPTY = 0x00000000,   /**< nothing has been returned */
    TAIL_ERRSTAT = 0x00000001, /**< db errstat */
    TAIL_EXTENDED = 0x40000000 /**< reserved to provide extension */
};

enum RMTDB_TYPE {
    RMTDB_LOCAL = 0,
    RMTDB_COMDB2_TAGGED = 1,
    RMTDB_COMDB2_UNTAGGED = 2,
    RMTDB_COMDBG = 3,
    RMTDB_COMDB2_REMCUR = 4
};

enum DB_METADATA {
    META_SCHEMA_RRN = 0, /* use this rrn in the meta table for schema info */
    META_SCHEMA_VERSION = 1, /* this key holds the current ONDISK schema version
                                as a 32 bit int */

    META_CSC2_RRN = -1,     /* for the csc2 versioning */
    META_CSC2_VERSION = -1, /* key for current version of csc2 file */

    META_CSC2DATE_RRN = -2, /* in this rrn we store the date as a unix epoch)
                               when a schema was loaded.  key is a schema
                               version number. */

    META_BLOBSTRIPE_GENID_RRN = -3, /* in this rrn store the genid of table
                                       when it was converted to blobstripe */

    META_STUFF_RRN = -4, /* used by pushlogs.c to do "stuff" to the database
                           until we get past a given lsn. */
    META_ONDISK_HEADER_RRN = -5,  /* do we have the new ondisk header? */
    META_COMPRESS_RRN = -6,       /* which compression algorithm to use for new
                                     records (if any) */
    META_COMPRESS_BLOBS_RRN = -7, /* and which to use for blobs. */
    META_FILEVERS = -8,           /* 64 bit id for filenames */
    META_FILE_LWM = -9,           /* int - lower deleteable log file */
    META_INSTANT_SCHEMA_CHANGE = -10,
    META_DATACOPY_ODH = -11,
    META_INPLACE_UPDATES = -12,
    META_BTHASH = -13,
    META_QUEUE_ODH = -14,
    META_QUEUE_COMPRESS = -15,
    META_QUEUE_PERSISTENT_SEQ = -16,
    META_QUEUE_SEQ = -17
};

enum CONSTRAINT_FLAGS {
    CT_UPD_CASCADE = 0x00000001,
    CT_DEL_CASCADE = 0x00000002,
    CT_BLD_SKIP    = 0x00000004,
    CT_DEL_SETNULL = 0x00000008,
    CT_NO_OVERLAP  = 0x00000010,
};

/* dbtable type specifier, please do not use for schema change type */
enum {
    UNUSED_1 = 0,
    DBTYPE_TAGGED_TABLE = 1,
    DBTYPE_QUEUE = 2,
    UNUSED_2 = 3,
    DBTYPE_QUEUEDB = 4
};

/* Copied verbatim from bdb_api.h since we don't expose that file to most
 * modules. */
enum {
    COMDB2_THR_EVENT_DONE_RDONLY = 0,
    COMDB2_THR_EVENT_START_RDONLY = 1,
    COMDB2_THR_EVENT_DONE_RDWR = 2,
    COMDB2_THR_EVENT_START_RDWR = 3
};

enum lclop {
    LCL_OP_ADD = 1,
    LCL_OP_DEL = 2,
    /* update goes out as del+add */
    LCL_OP_COMMIT = 3,
    LCL_OP_TOOBIG = 4, /* internal to receiver, not used in comdb2 */
    LCL_OP_CLEAR = 5,
    LCL_OP_ANALYZE = 6
};

enum { DB_COHERENT = 0, DB_INCOHERENT = 1 };

enum SQL_TRANLEVEL_DEFAULT {
    SQL_TDEF_COMDB2 = 0x00000,   /* "comdb2" */
    SQL_TDEF_BLOCK = 0x01000,    /* "block" */
    SQL_TDEF_SOCK = 0x02000,     /* "blocksock" */
    SQL_TDEF_RECOM = 0x04000,    /* "recom" */
    SQL_TDEF_SNAPISOL = 0x08000, /* "snapshot isolation" */
    SQL_TDEF_SERIAL = 0x10000    /* "serial" */
};

enum RECORD_WRITE_TYPES {
    RECORD_WRITE_INS = 0,
    RECORD_WRITE_UPD = 1,
    RECORD_WRITE_DEL = 2,
    RECORD_WRITE_MAX = 3
};

enum RECOVER_DEADLOCK_FLAGS {
    RECOVER_DEADLOCK_PTRACE = 0x00000001,
    RECOVER_DEADLOCK_FORCE_FAIL = 0x00000002,
    RECOVER_DEADLOCK_IGNORE_DESIRED= 0x00000004
};

enum CURTRAN_FLAGS { CURTRAN_RECOVERY = 0x00000001 };

/* Raw stats, kept on a per origin machine basis.  Please don't add any other data
 * type above `svc_time' as this allows us to easily sum it and diff it in a
 * loop in reqlog.c.
 */
struct rawnodestats {
    unsigned opcode_counts[MAXTYPCNT];
    unsigned blockop_counts[NUM_BLOCKOP_OPCODES];
    unsigned sql_queries;
    unsigned sql_steps;
    unsigned sql_rows;

    struct time_metric *svc_time; /* <-- offsetof */

    pthread_mutex_t lk;
    hash_t *fingerprints;
    api_history_t *api_history;
};
#define NUM_RAW_NODESTATS                                                      \
    (offsetof(struct rawnodestats, svc_time) / sizeof(unsigned))

struct summary_nodestats {
    int node;
    char *host;
    struct in_addr addr;
    char *task;
    char *stack;
    int ref;
    int is_ssl;

    unsigned finds;
    unsigned rngexts;
    unsigned writes;
    unsigned other_fstsnds;

    unsigned adds;
    unsigned upds;
    unsigned dels;
    unsigned bsql;     /* block sql */
    unsigned recom;    /* recom sql */
    unsigned snapisol; /* snapisol sql */
    unsigned serial;   /* serial sql */

    unsigned sql_queries;
    unsigned sql_steps;
    unsigned sql_rows;
    double svc_time;
};

/* records in sql master db look like this (no appended rrn info) */
struct sqlmdbrectype {
    char type[8];
    char name[20];
    char tblname[20];
    int rootpage;
    char sql[876];
};

/* This is the transparent seqnum type, which should match the bdb_api defined
 * type in size.  backend_open() has a sanity check to enforce this.  */
typedef int db_seqnum_type[10];

enum { COMDB2_NULL_TYPE = -1 };

/* All the data needed for a bulk import */
typedef struct bulk_import_data bulk_import_data_t;
struct bulk_import_data {
    unsigned long long data_genid;
    unsigned long long index_genids[MAXINDEX];
    unsigned long long blob_genids[MAXBLOBS];

    char table_name[MAXTABLELEN];

    /* not check for equality in bulk_import_data_validate since it doesn't need
     * to be the same on all machines */
    char data_dir[256 /*arbitrary, must be able to fit any data dir*/];

    unsigned csc2_crc32;

    int checksums;

    int odh;
    int compress;
    int compress_blobs;

    int dtastripe;
    int blobstripe;
    size_t num_index_genids;
    size_t num_blob_genids;

    int filenames_provided;
    char *data_files[MAXDTASTRIPE];
    char *index_files[MAXINDEX];
    char *blob_files[MAXBLOBS][MAXDTASTRIPE];
    int bulk_import_version;
};

struct dbstore {
    uint8_t ver;
    int len;
    void *data;
};

typedef struct timepart_views timepart_views_t;

#define consumer_lock_read(x) consumer_lock_read_int(x, __func__, __LINE__);
void consumer_lock_read_int(struct dbtable *db, const char *func, int line);

#define consumer_lock_write(x) consumer_lock_write_int(x, __func__, __LINE__);
void consumer_lock_write_int(struct dbtable *db, const char *func, int line);

#define consumer_unlock(x) consumer_unlock_int(x, __func__, __LINE__);
void consumer_unlock_int(struct dbtable *db, const char *func, int line);

/*
 * We now have different types of db (I overloaded this structure rather than
 * create a new structure because the ireq usedb concept is endemic anyway).
 */
typedef struct dbtable {
    struct dbenv *dbenv; /*chain back to my environment*/
    char *lrlfname;
    char *tablename;
    char *sqlaliasname;
    struct ireq *iq; /* iq used at sc time */

    int dbnum; /* zero unless setup as comdbg table */
    int lrl; /*dat len in bytes*/
    /*index*/
    unsigned short nix; /*number of indices*/

    unsigned short ix_keylen[MAXINDEX]; /*key len in bytes*/
    signed char ix_dupes[MAXINDEX];
    signed char ix_recnums[MAXINDEX];
    signed char ix_datacopy[MAXINDEX];
    int ix_datacopylen[MAXINDEX]; /* datacopy len in bytes (0 if full datacopy) */
    signed char ix_collattr[MAXINDEX];
    signed char ix_nullsallowed[MAXINDEX];

    shard_limits_t *sharding;

    int numblobs;

    /* we do not necessarily have as many sql indexes as there are comdb2
     * indexes - only indexes free of <DESCEND> can be advertised to sqlite.
     * this for some values of n, ixsql[n] might be NULL.  nsqlix is the count
     * of indexes that are exposed to sqlite. */
    struct schema *schema;
    struct schema **ixschema;
    char *sql;
    char **ixsql;
    int nsqlix;

    /*backend db engine handle*/
    bdb_state_type *handle;

    /* meta-data.  this may be a lite db.  it may be NULL, because older
     * comdb2s didn't have meta dbs.  also it may be NULL if the dbenv
     * meta handle is non-NULL - the new approach is one meta table per
     * database a sthis scales much better as we add more tables. */
    void *meta;

    /*counters*/
    int64_t typcnt[MAXTYPCNT + 1];
    int64_t blocktypcnt[BLOCK_MAXOPCODE];
    int64_t blockosqltypcnt[MAX_OSQL_TYPES];
    int64_t nsql;  // counter for queries to this table 
    /*prev counters for diff*/
    int64_t prev_typcnt[MAXTYPCNT + 1];
    int64_t prev_blocktypcnt[BLOCK_MAXOPCODE];
    int64_t prev_blockosqltypcnt[MAX_OSQL_TYPES];
    int64_t prev_nsql;
    /* counters for writes to this table */
    int64_t write_count[RECORD_WRITE_MAX];
    int64_t saved_write_count[RECORD_WRITE_MAX];
    /* counters for cascaded writes to this table */
    int64_t casc_write_count;
    int64_t saved_casc_write_count;
    int64_t deadlock_count;
    int64_t saved_deadlock_count;
    int64_t aa_saved_counter; // zeroed out at autoanalyze
    int64_t aa_lastepoch;
    int64_t aa_needs_analyze_time; // time when analyze is needed for table in request mode, otherwise 0
    int64_t read_count; // counter for reads to this table
    int64_t index_used_count;   // counter for number of times a table index was used

    /* Foreign key constraints */
    constraint_t *constraints;
    size_t n_constraints;
    /* Pointers to other table constraints that are directed at this table. */
    constraint_t **rev_constraints;
    size_t n_rev_constraints;
    size_t cap_rev_constraints;
    pthread_mutex_t rev_constraints_lk;

    /* CHECK constraints */
    check_constraint_t *check_constraints;
    size_t n_check_constraints;
    char **check_constraint_query;

    /* One of the DBTYPE_ constants. */
    int dbtype;

    struct consumer *consumers[MAXCONSUMERS];

    /* Expected average size of a queue item in bytes. */
    int avgitemsz;
    int queue_pagesize_override;

    /* some queue stats */
    unsigned int num_goose_adds;
    unsigned int num_goose_consumes;

    /* used by the queue goosing sub system */
    int goose_consume_cnt;
    int goose_add_cnt;

    /* needed for foreign table support */
    int dtastripe;

    /* when we were blobstriped */
    unsigned long long blobstripe_genid;

    /* placeholder for storing file sizes when we need them */
    uint64_t ixsizes[MAXINDEX];
    uint64_t dtasize;
    uint64_t blobsizes[MAXBLOBS];
    uint64_t totalsize;
    unsigned numextents;

    /* index stats */
    unsigned long long *ixuse;
    unsigned long long *sqlixuse;

    /* Used during schema change to describe how we will form the new table
     * from the old table.  The add/update/delete routines will only look at
     * this information if plan!=NULL.
     * To keep things sane we always create the full complement of dtafile,
     * ix files and blob files for a new table.  If a plan is in use then
     * we may not populate all of them.  Slightly wasteful, but for now this
     * seems safer and more likely not to mess up.
     * This probably doesn't work that great with constraints (yet).
     */
    struct scplan *plan;
    int dbs_idx; /* index of us in dbenv->dbs[] */

    struct dbtable *sc_from; /* point to the source db, replace global sc_from */
    struct dbtable *sc_to; /* point to the new db, replace global sc_to */

    int sc_live_logical;
    unsigned long long *sc_genids; /* schemachange stripe pointers */

    /* All writer threads have to grab the lock in read/write mode.  If a live
     * schema change is in progress then they have to do extra stuff. */
    pthread_rwlock_t sc_live_lk;

    /* count the number of updates and deletes done by schemachange
     * when behind the cursor.  This helps us know how many
     * records we've really done (since every update behind the cursor
     * effectively means we have to go back and do that record again). */
    uint32_t sc_adds;
    uint32_t sc_deletes;
    uint32_t sc_updates;

    uint64_t sc_nrecs;
    uint64_t sc_prev_nrecs;
    unsigned int sqlcur_ix;  /* count how many cursors where open in ix mode */
    unsigned int sqlcur_cur; /* count how many cursors where open in cur mode */

    char *csc2_schema;
    int csc2_schema_len;

    struct dbstore dbstore[MAXCOLUMNS];
    int odh;
    /* csc2 schema version increased on instantaneous schemachange */
    int schema_version;
    int instant_schema_change;
    int inplace_updates;
    /* tableversion is an ever increasing counter which is incremented for
     * every schema change (add, alter, drop, etc.) but not for fastinit */
    unsigned long long tableversion;

    /* map of tag fields for schema version to curr schema */
    unsigned int * versmap[MAXVER + 1];
    /* is tag version compatible with ondisk schema */
    uint8_t vers_compat_ondisk[MAXVER + 1];

    /* lock for consumer list */
    pthread_rwlock_t consumer_lk;

    unsigned has_datacopy_ix : 1; /* set to 1 if we have datacopy indexes */
    unsigned ix_partial : 1;      /* set to 1 if we have partial indexes */
    unsigned ix_expr : 1;         /* set to 1 if we have indexes on expressions */
    unsigned ix_blob : 1;         /* set to 1 if blobs are involved in indexes */
    unsigned ix_func : 1;         /* set to 1 if sfuncs are involved in indexes */

    char ** lua_sfuncs;           /* The lua scalar functions used by indexes in this table*/
    int num_lua_sfuncs;           /* The number of lua scalar functions used by indexes in this table*/

    unsigned sc_abort : 1;
    unsigned sc_downgrading : 1;

    /* boolean value set to nonzero if table rebuild is in progress */
    unsigned doing_conversion : 1;
    /* boolean value set to nonzero if table upgrade is in progress */
    unsigned doing_upgrade : 1;

    unsigned disableskipscan : 1;
    unsigned do_local_replication : 1;

    /* name of the timepartition, if this is a shard */
    const char *timepartition_name;
} dbtable;

struct dbview {
    char *view_name;
    char *view_def;
};

struct log_delete_state {
    int filenum;
    LINKC_T(struct log_delete_state) linkv;
};

struct lrlfile {
    char *file;
    LINKC_T(struct lrlfile) lnk;
};

struct lrl_handler {
    int (*handle)(struct dbenv*, const char *line);
    LINKC_T(struct lrl_handler) lnk;
};

struct message_handler {
    int (*handle)(struct dbenv*, const char *line);
    LINKC_T(struct message_handler) lnk;
};

struct dbenv {
    char *basedir;
    char *envname;
    int dbnum;

    /*backend db engine handle*/
    void *bdb_attr;     /*engine attributes*/
    void *bdb_callback; /*engine callbacks */

    char *master; /*current master node, from callback*/
    int gen;      /*generation for current master node*/
    int egen;     /*election generation for current master node*/

    int cacheszkb;
    int cacheszkbmin;
    int cacheszkbmax;
    int override_cacheszkb;

    /*sibling info*/
    int nsiblings;
    char *sibling_hostname[REPMAX];
    unsigned short sibling_port[REPMAX][NET_MAX];
    int listen_fds[NET_MAX];
    /* banckend db engine handle for replication */
    void *handle_sibling;
    void *handle_sibling_offload;

    /*replication sync mode */
    int rep_sync;

    /*log sync mode */
    int log_sync;
    int log_sync_time;
    int log_mem_size;
    /*log deletion is now sort of reference counted to allow socket
     *applications to keep it held turned off during a backup. */
    int log_delete;
    pthread_mutex_t log_delete_counter_mutex;
    /* the log delete age is the epoch time after which log files can't
     * be deleted.  if this is <=0 then we will always delete archiveable
     * log files if log deletion is on. */
    int log_delete_age;
    /* the log delete filenum is the highest log file number that may
     * be removed.  set this to -1 if any log file may be removed. */
    int log_delete_filenum;
    /* this is a linked list of log_delete_stat structs. If the list is
     * empty then log file deletion can proceed as normal. Otherwise we
     * have one or more clients that have requested log deletion to be
     * held up, at least beyond a certain log number. The log_delete_state
     * structs themselves reside on the stacks of appsock threads. */
    LISTC_T(struct log_delete_state) log_delete_state_list;

    /*counters*/
    int typcnt[MAXTYPCNT + 1];

    /* bdb_environment */
    bdb_state_type *bdb_env;

    /* Tables */
    int num_dbs;
    dbtable **dbs;
    dbtable static_table;
    hash_t *db_hash;
    hash_t *sqlalias_hash;

    /* Queues */
    int num_qdbs;
    struct dbtable **qdbs;
    hash_t *qdb_hash;

    /* Views */
    hash_t *view_hash;

    LISTC_T(struct lua_func_t) lua_sfuncs;
    LISTC_T(struct lua_func_t) lua_afuncs;

    /* is sql mode enabled? */
    int sql;

    /* enable client side retrys for N seconds */
    int retry;

    int rep_always_wait;

    pthread_t purge_old_blkseq_tid;
    pthread_t purge_old_files_tid;

    /* stupid - is the purge_old_blkseq thread running? */
    int purge_old_blkseq_is_running;
    int purge_old_files_is_running;
    int stopped; /* set when exiting -- if set, drop requests */
    int no_more_sql_connections;

    LISTC_T(struct sql_thread) sql_threads;
    LISTC_T(struct sql_hist) sqlhist;

    hash_t *long_trn_table; /* h-table of long transactions--fast lookup */
    struct long_trn_stat long_trn_stats;
    pthread_mutex_t long_trn_mtx;

    /* the per database meta table */
    void *meta;

    /* Replication stats (these are updated locklessly so may display
     * gibberish).  We only count successful commits. */
    uint64_t biggest_txn;
    uint64_t total_txn_sz;
    int num_txns;
    int max_timeout_ms;
    int total_timeouts_ms;
    int max_reptime_ms;
    int total_reptime_ms;

    /* gathered at startup time and processed */
    char **allow_lines;
    int num_allow_lines;
    int max_allow_lines;
    int errstaton;

    sqlpool_t sqlthdpool;

    prefaultiopool_type prefaultiopool;
    prefault_helper_type prefault_helper;
    readaheadprefault_type readahead_prefault;

    prefault_stats_type prefault_stats;

    int lowdiskpercent; /* % full at which disk space considered dangerous */

    void *dl_cache_heap;
    mspace dl_cache_mspace;

    pthread_mutex_t incoherent_lk;
    int num_incoherent;
    int fallen_offline;
    int manager_dbnum;

    pthread_t watchdog_tid;
    pthread_t watchdog_watcher_tid;

    int64_t txns_committed;
    int64_t txns_aborted;
    int64_t prev_txns_committed;
    int64_t prev_txns_aborted;
    int wait_for_N_nodes;

    LISTC_T(struct lrlfile) lrl_files;

    int incoh_notcoherent;
    uint32_t incoh_file, incoh_offset;
    timepart_views_t *timepart_views;

    struct time_metric *service_time;
    struct time_metric *queue_depth;
    struct time_metric *concurrent_queries;
    struct time_metric *connections;
    struct time_metric *sql_queue_time;
    struct time_metric *handle_buf_queue_time;
    struct time_metric *watchdog_time;
    LISTC_T(struct lrl_handler) lrl_handlers;
    LISTC_T(struct message_handler) message_handlers;

    comdb2_queue_consumer_t *queue_consumer_handlers[CONSUMER_TYPE_LAST];
};

extern struct dbenv *thedb;
extern comdb2_tunables *gbl_tunables;

extern pthread_key_t thd_info_key;
extern pthread_key_t query_info_key;

struct req_hdr {
    int ver1;
    short ver2;
    unsigned char luxref;
    unsigned char opcode;
};

enum { REQ_HDR_LEN = 4 + 2 + 1 + 1 };

enum {
    COMDBG_FLAG_FROM_LE = 1 << 16
};

BB_COMPILE_TIME_ASSERT(req_hdr_len, sizeof(struct req_hdr) == REQ_HDR_LEN);

struct thread_info {
    long long uniquetag;
    long long ct_id_key;
    void *ct_add_table;
    void *ct_del_table;
    void *ct_add_index;
    void *stmt_cache;
    hash_t *ct_add_table_genid_hash; // for quick lookups
    pool_t *ct_add_table_genid_pool; // provides memory for the above hash
};

/* key for fstblk records - a 12 byte sequence number generated by the
 * proxy and unique for each transaction. */
typedef struct {
    int int3[3];
} fstblkseq_t;

/* Query cost stats as they go down to the client */
struct client_query_path_component {
    int nfind;
    int nnext;
    int nwrite;
    char table[32];
#if 0
    [32+1/*.*/+31/*dbname*/];
#endif
    int ix;
};

enum { CLIENT_QUERY_PATH_COMPONENT_LEN = 4 + 4 + 4 + (1 * 32) + 4 };

BB_COMPILE_TIME_ASSERT(client_query_path_component_len,
                       sizeof(struct client_query_path_component) ==
                           CLIENT_QUERY_PATH_COMPONENT_LEN);

typedef struct {
    char *tbname;
    int idxnum;
    void *lkey;
    void *rkey;
    int lflag;
    int lkeylen;
    int rflag;
    int rkeylen;
    int islocked;
} CurRange;

typedef struct {
    int size;
    int cap;
    unsigned int file;
    unsigned int offset;
    hash_t *hash;
    CurRange **ranges;
} CurRangeArr;

struct serial_tbname_hash {
    char *tbname;
    int islocked;
    int size;
    int begin;
    int end;
    hash_t *idx_hash;
};

struct serial_index_hash {
    int idxnum;
    int size;
    int begin;
    int end;
};

struct client_query_stats {
    int queryid;
    int nlocks;
    int n_write_ios;
    int n_read_ios;
    int reserved[16];
    int n_rows;
    int n_components;
    double cost;
    struct client_query_path_component path_stats[1];
};

enum {
    CLIENT_QUERY_STATS_PATH_OFFSET = 4 + 4 + 4 + 4 + (4 * 16) + 4 + 4 + 8,
    CLIENT_QUERY_STATS_LEN =
        CLIENT_QUERY_STATS_PATH_OFFSET + CLIENT_QUERY_PATH_COMPONENT_LEN
};

BB_COMPILE_TIME_ASSERT(client_query_stats_len,
                       offsetof(struct client_query_stats, path_stats) ==
                           CLIENT_QUERY_STATS_PATH_OFFSET);

typedef struct osql_bp_timings {
    unsigned long long
        req_received; /* first osql bp encountered in block tran */
    unsigned long long req_alldone; /* all sql sessions confirmed finished */
    unsigned long long
        req_applied; /* all changes were applied with non-retriably outcome */
    unsigned long long
        req_sentrc; /* rc was sent back to sql- for non-blocksql */
    unsigned long long req_finished;      /* all processing is complete */
    unsigned long long replication_start; /* time replication */
    unsigned long long replication_end;   /* time replication */
    unsigned int retries;                 /* retried bplog transaction */
} osql_bp_timings_t;

struct query_effects {
    int num_affected;
    int num_selected;
    int num_updated;
    int num_deleted;
    int num_inserted;
};

enum {
    OSQL_NET_SNAP_FOUND_UID = 1,     /* Give old response to client. */
    OSQL_NET_SNAP_NOT_FOUND_UID = 2, /* Ask client to retry. */
    OSQL_NET_SNAP_ERROR = 3          /* Server isn't setup properly. */
};

#define MAX_SNAP_KEY_LEN 64

typedef struct snap_uid {
    uuid_t uuid; /* wait for the reply */
    int rqtype;  /* add/check */
    struct query_effects effects;
    uint16_t unused;
    uint8_t replicant_is_able_to_retry; /* verifyretry on && !snapshot_iso or
                                           higer */
    uint8_t keylen;
    char key[MAX_SNAP_KEY_LEN]; /* cnonce */
} snap_uid_t;

enum { SNAP_UID_LENGTH = 16 + 4 + (4 * 5) + 4 + 64 };

BB_COMPILE_TIME_ASSERT(snap_uid_size, sizeof(snap_uid_t) == SNAP_UID_LENGTH);

/*
   lrl tunables that control this:
   querylimit [warn] maxcost #
   querylimit [warn] tablescans yes/no
   querylimit [warn] temptables yes/no

   By default everything is allowed.  Permissions are considered in order.
   comdb2.lrl is processed first, then comdb2_local.lrl, then the db's lrl file,
   then any 'send' commands to the db, then any options set on a connection.
*/
struct query_limits {
    double maxcost;
    int tablescans_ok;
    int temptables_ok;

    double maxcost_warn;
    int tablescans_warn;
    int temptables_warn;
};
enum { QUERY_LIMITS_LEN = 8 + 4 + 4 + 8 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(query_limits_size,
                       sizeof(struct query_limits) == QUERY_LIMITS_LEN);

/* We have 2 flavors of sequence numbers.  An older one is 3 integers.  A newer
 * one is
 * a uuid. */
enum { MAX_SEQ_LEN = 16 };

/* internal request tracking */
enum { IREQ_MAX_PREFIXES = 16 };

/******************** BPFUNC FORWARD DECLARATIONS *****************/
struct bpfunc;
typedef struct bpfunc bpfunc_t;

typedef struct bpfunc_lstnode {
    bpfunc_t *func;
    LINKC_T(struct bpfunc_lstnode) linkct;

} bpfunc_lstnode_t;

typedef LISTC_T(bpfunc_lstnode_t) bpfunc_list_t;

struct participant {
    char *participant_name;
    char *participant_tier;
    char *participant_master;
    time_t last_heartbeat;
    int status;
    pthread_mutex_t lk;
    LINKC_T(struct participant) linkv;
};

typedef LISTC_T(struct participant) participant_list_t;

/*******************************************************************/

enum OSQL_REQ_TYPE {
    OSQL_REQINV = 0,
    OSQL_BLOCK_REQ = 1, /* obsolete */
    OSQL_SOCK_REQ = 2,
    OSQL_RECOM_REQ = 3,
    OSQL_SERIAL_REQ = 4,

    OSQL_BLOCK_REQ_COST = 5, /* obsolete */
    OSQL_SOCK_REQ_COST = 6,

    OSQL_SNAPISOL_REQ = 7,
    OSQL_SNAP_UID_REQ = 8,
    OSQL_MAX_REQ = 9,
};

#define IQ_SNAPINFO(iq) ((iq)->sorese->snap_info)
#define IQ_HAS_SNAPINFO(iq) ((iq)->sorese && (iq)->sorese->snap_info)
#define IQ_HAS_SNAPINFO_KEY(iq) (IQ_HAS_SNAPINFO(iq) && IQ_SNAPINFO(iq)->keylen > 0)

/* Magic rqid value that means "please use uuid instead" */
#define OSQL_RQID_USE_UUID 1
typedef struct blocksql_tran blocksql_tran_t;
typedef struct sess_impl sess_impl_t;

enum osql_target_type { OSQL_OVER_NET = 1, OSQL_OVER_SOCKET = 2 };
struct osql_target {
    enum osql_target_type type;
    unsigned is_ondisk;
    const char *host;
    SBUF2 *sb;
    int (*send)(struct osql_target *target, int usertype, void *data,
                int datalen, int nodelay, void *tail, int tailen);
};
typedef struct osql_target osql_target_t;

struct osql_sess {

    /* request part */
    unsigned long long rqid; /* identifies the client request session */
    uuid_t uuid;
    snap_uid_t *snap_info;
    sess_impl_t *impl;
    struct ireq *iq; /* iq used by block processor thread */

    char tzname[DB_MAX_TZNAMEDB]; /* tzname used for this request */

    enum OSQL_REQ_TYPE type; /* session version */

    struct errstat xerr; /* error set when OSQL_XERR arrives */

    const char *sql; /* if set, pointer to sql string (part of req) */

    /* this are set for each session retry */
    int64_t sess_startus;   /* when this was first started */
    int64_t sess_endus;     /* when this was complete */
    unsigned int tran_rows; /* number of rows that are actual ADD/UPD/DEL */

    int queryid;
    unsigned is_reorder_on : 1;
    unsigned is_delayed : 1;

    /* from sorese */
    osql_target_t target; /* replicant machine; host is NULL if local */
    int nops;         /* if no error, how many updated rows were performed */
    int rcout;        /* store here the block proc main error */

    int verify_retries; /* how many times we verify retried this one */
    blocksql_tran_t *tran;
    LISTC_T(struct schema_change_type) scs; /* schema changes in session */
    int is_tptlock;   /* needs tpt locking */
    int is_cancelled; /* 1 if session is cancelled */

    /* 2pc maintained in session */
    unsigned is_participant : 1;
    unsigned is_coordinator : 1;

    char *dist_txnid;
    char *coordinator_dbname;
    char *coordinator_tier;
    char *coordinator_master;
    int64_t dist_timestamp;
    participant_list_t participants;

    /* these are set asynchronously */
    pthread_mutex_t participant_lk;
    int is_done;
    int is_sanctioned; /* set by fdb from coordinator-master */
};
typedef struct osql_sess osql_sess_t;

struct llog_scdone;
struct ireq {
    /* bzero-ing this entire struct was turning out to be very expensive.
     * So organizing this into 3 regions:
     * 1. variables that are set to non zero values
     * 2. char[] variables which just need var[0] to be set to NULL
     * 3. variables that are set to zero (bzero candidate)
     */

    /************/
    /* REGION 1 */
    /************/
    char *frommach; /* machine that the buffer came from */
    const char *where;
    char *gluewhere; /*backend code where*/
    uint64_t debug_now;
    uint64_t nowus; /*received.*/
    struct query_limits __limits;
    struct dbenv *dbenv;
    struct dbtable *origdb;
    struct dbtable *usedb;

    /* IPC stuff */
    void *p_sinfo;
    intptr_t curswap; /* 040307dh: 64bit */

    /* these usually refer to diffent points in the same fstsnd buffer, as such
     * p_buf_in and p_buf_in_end should be set to NULL once writing to p_buf_out
     * has begun */
    void *p_buf_orig;        /* save original buffer that is saved in blkstate
                                to allow fast fail with no blockprocessor */
    const uint8_t *p_buf_in; /* pointer to current pos in input buf */
    const uint8_t *p_buf_in_end;  /* pointer to just past end of input buf */
    uint8_t *p_buf_out;           /* pointer to current pos in output buf */
    uint8_t *p_buf_out_start;     /* pointer to start of output buf */
    const uint8_t *p_buf_out_end; /* pointer to just past end of output buf */
    unsigned long long fwd_tag_rqid;
    int frompid;
    int debug;
    int opcode;
    int luxref;
    uint8_t osql_rowlocks_enable;
    uint8_t osql_genid48_enable;

    int commit_file;
    int commit_offset;

    /************/
    /* REGION 2 */
    /************/
    uint8_t region2; /* used for offsetof */
    char corigin[80];
    char debug_buf[256];
    char tzname[DB_MAX_TZNAMEDB];

    /************/
    /* REGION 3 */
    /************/
    uint8_t region3; /* used for offsetof */

    uint64_t startus; /* thread handling; start time stamp */
    /* for waking up socket thread. */
    void *request_data;
    char *tag;

    errstat_t errstat;
    struct javasp_trans_state *jsph;
    struct block_state *blkstate;
    struct schema_change_type *sc;

    bpfunc_list_t bpfunc_lst;

    struct thread_info *thdinfo;

    struct dbtable *queues_hit[MAX_QUEUE_HITS_PER_TRANS];

    /* List of replication objects associated with the ireq/transaction
     * which other subsystems (i.e. queues) may need to wait for. */
    struct repl_object *repl_list;

    SBUF2 *sb; /*NULL, or valid for socket requests */

    /* if this is not NULL then we might want logging for this request.. */
    struct reqlogger *reqlogger;

    /* The stats for the origin node of this request (can be NULL) */
    struct rawnodestats *rawnodestats;

    /* copy of blkseq */
    uint8_t seq[MAX_SEQ_LEN];

    /* socksql/recom storage */
    osql_sess_t *sorese;

    /* debug osql */
    osql_bp_timings_t timings;

    /* Support for //DBSTATS. */
    SBUF2 *dbglog_file;
    int *nwrites;

    /* List of indices that we've written to detect uncommittable upsert txns */
    hash_t *vfy_idx_hash; 

    int dup_key_insert;

    /* List of genids that we've written to detect uncommittable txn's */
    hash_t *vfy_genid_hash;
    pool_t *vfy_genid_pool;

    /* read-set validation */
    CurRangeArr *arr;
    CurRangeArr *selectv_arr;

    /* indexes on expressions */
    uint8_t **idxInsert;
    uint8_t **idxDelete;

    /* osql prefault step index */
    int *osql_step_ix;

    tran_type *sc_logical_tran;
    tran_type *sc_tran;
    tran_type *sc_close_tran;
    struct schema_change_type *sc_pending;
    LISTC_T(struct schema_change_type) scs; /* all schema changes in this txn */
    double cost;
    uint64_t sc_seed;
    uint32_t sc_host;

    uint64_t txnsize;
    uint64_t total_txnsize;
    unsigned long long last_genid;
    uint64_t txn_ttl_ms; /* txn time to live -- abort after this time */

    /* if we replicated then these get updated */
    int reptimems;
    int timeoutms;

    /* more stats - number of retries done under this request */
    uint32_t retries;

    int ixused;    /* what index was used? */
    int ixstepcnt; /* how many steps on that index? */

    /* which queues were added to? (so we can wake up their consumer
     * threads).  num_queues_hit==MAX_QUEUE_HITS_PER_TRANS+1 means that
     * we'll have to wake up all queues on commit - oh well. */
    unsigned num_queues_hit;

    /* Number of oplog operations logged as part of this transaction */
    int oplog_numops;
    int seqlen;
    int helper_thread;
    int queryid;
    int osql_flags;
    uint32_t priority;
    int tranddl;
    int tptlock; /* need to know if we need to wrap whole txn in a view_lock */
    uint32_t written_row_count;
    uint32_t cascaded_row_count;

    /* Client endian flags. */
    uint8_t client_endian;

    unsigned have_client_endian : 1;
    unsigned is_fake : 1;
    unsigned is_dumpresponse : 1;
    unsigned is_fromsocket : 1;
    unsigned is_socketrequest : 1;
    unsigned is_block2positionmode : 1;

    unsigned errstrused : 1;
    unsigned vfy_genid_track : 1;
    unsigned vfy_idx_track : 1;
    unsigned have_blkseq : 1;

    unsigned sc_locked : 1;
    unsigned sc_should_abort : 1;
    unsigned sc_closed_files : 1;

    int sc_running;
    int comdbg_flags;

    int64_t timestamp;
    /* REVIEW COMMENTS AT BEGINING OF STRUCT BEFORE ADDING NEW VARIABLES */
};

/* comdb array struct */
struct array {
    int rcode;
    int zero[2];
    int rrn;
    char key[64]; /*58 byte key + 6 bytes of fluff (was btree data in comdbg)*/
};

enum { COMDBG_KEY_GENID_OFFSET = 56 };

enum { ARRAY_LEN = 4 + (2 * 4) + 4 + (1 * 64) };

BB_COMPILE_TIME_ASSERT(array_len, sizeof(struct array) == ARRAY_LEN);

/* endianized array_getter for rngext */
const uint8_t *array_get(struct array *p_array, const uint8_t *p_buf,
                         const uint8_t *p_buf_end, int comdbg_flags);

/* endianized array_setter for rngext */
uint8_t *array_put(const struct array *p_array, uint8_t *p_buf,
                   const uint8_t *p_buf_end, int comdbg_flags);

// TODO: move to its own include
#define GETFUNC \
    uint8_t* (*getfunc)(void *v_dst, size_t len, const void *v_src, const void *v_end); \
    if (comdbg_flags & COMDBG_FLAG_FROM_LE) \
        getfunc = buf_little_get_func; \
    else \
        getfunc = buf_get_func;

#define PUTFUNC \
    uint8_t* (*putfunc)(const void *v_src, size_t len, void *v_dst,  const void *v_end); \
    if (comdbg_flags & COMDBG_FLAG_FROM_LE) \
        putfunc = buf_little_put_func; \
    else \
        putfunc = buf_put_func;



/* rng_segment array- used for range extract calls in torngextx and torngext2 */
struct rng_segment {
    int off;
    int len;
};

enum { RNG_SEGMENT_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(rng_segment_len,
                       sizeof(struct rng_segment) == RNG_SEGMENT_LEN);

/* retrieve a network-ordered range-extract segment descriptor */
const uint8_t *rng_segment_get(struct rng_segment *p_segments,
                               const uint8_t *p_buf, const uint8_t *p_buf_end,
                               int comdbg_flags);

/* key for historical meta db records */
struct metahdr {
    int rrn;
    int attr;
};

enum { METAHDR_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(metahdr_len, sizeof(struct metahdr) == METAHDR_LEN);

struct metahdr2 {
    /* for historical "per table" records this should be "/<tablename>" */
    char keystr[120];
    struct metahdr hdr1;
}; /* 128 bytes */

enum { METAHDR2_LEN = 120 + sizeof(struct metahdr) };

BB_COMPILE_TIME_ASSERT(metahdr2_len, sizeof(struct metahdr2) == METAHDR2_LEN);

/* used in some requests to store schema blob information. */
struct client_blob_type;
typedef struct {
    int numcblobs;
    int cblob_disk_ixs[MAXBLOBS];
    int cblob_tag_ixs[MAXBLOBS];
    struct client_blob_type *blobflds[MAXBLOBS];
    size_t bloblens[MAXBLOBS];
    size_t bloboffs[MAXBLOBS];
    char *blobptrs[MAXBLOBS];
    size_t total_length;
} blob_status_t;

/* convert all records scan modes */
enum convert_scan_mode {
    SCAN_INDEX = 0,   /* default for old style schema changes */
    SCAN_STRIPES = 1, /* requires dtastripe, required for live schema change */
    SCAN_DUMP = 2,
    SCAN_OLDCODE = 3,
    SCAN_PARALLEL = 4, /* creates one thread for each stripe */
    SCAN_PAGEORDER = 5 /* 1 thread per stripe in page-order */
};

typedef struct {
    unsigned long long rqid;
    unsigned step;
    uuid_t uuid;
} osqlpf_step;

/* global settings */
extern int gbl_sc_timeoutms;
extern int gbl_trigger_timepart;
extern int gbl_multitable_ddl;

extern int64_t gbl_num_auth_allowed;
extern int64_t gbl_num_auth_denied;

extern const char *const gbl_db_git_version_sha;
extern const char gbl_db_version[];
extern const char gbl_db_semver[];
extern const char gbl_db_codename[];
extern const char gbl_db_buildtype[];
extern int gbl_sc_del_unused_files_threshold_ms;

extern int gbl_verbose_toblock_backouts;
extern int gbl_dispatch_rep_preprocess;
extern int gbl_dispatch_rowlocks_bench;
extern int gbl_rowlocks_bench_logical_rectype;

extern int gbl_morecolumns;
extern int gbl_maxreclen;
extern int gbl_penaltyincpercent;
extern int gbl_maxwthreadpenalty;

extern int gbl_uses_password;
extern int gbl_unauth_tag_access;
extern int gbl_uses_externalauth;
extern int gbl_uses_externalauth_connect;
extern int gbl_externalauth_warn;
extern int gbl_identity_cache_max;
extern int gbl_iam_verbosity;
extern char* gbl_foreign_metadb;
extern char* gbl_foreign_metadb_class;
extern char* gbl_foreign_metadb_config;


extern int gbl_upd_key;
extern unsigned long long gbl_sqltick;
extern int gbl_nonames;
extern int gbl_reject_osql_mismatch;
extern int gbl_abort_on_clear_inuse_rqid;

extern int gbl_exit_on_pthread_create_fail;
extern int gbl_exit_on_internal_error;

extern int gbl_osql_max_throttle_sec;
extern int gbl_throttle_sql_overload_dump_sec;
extern int gbl_heartbeat_check;
extern int gbl_osql_bkoff_netsend_lmt;
extern int gbl_osql_bkoff_netsend;
extern int gbl_nullfkey;
extern int gbl_prefaulthelper_blockops;
extern int gbl_prefaulthelper_sqlreadahead;
extern int gbl_maxblockops;
extern int gbl_rangextunit;
extern int gbl_honor_rangextunit_for_old_apis;
extern int gbl_sqlreadahead;
extern int gbl_sqlreadaheadthresh;
extern int gbl_iothreads;
extern int gbl_ioqueue;
extern int gbl_prefaulthelperthreads;

extern int gbl_osqlpfault_threads;
extern osqlpf_step *gbl_osqlpf_step;
extern queue_type *gbl_osqlpf_stepq;

extern int gbl_starttime;
extern int gbl_early_blkseq_check;

extern int gbl_repchecksum;
extern int gbl_pfault;
extern int gbl_pfaultrmt;
extern int gbl_dtastripe;
extern int gbl_blobstripe;
extern int gbl_debug;        /* enable operation debugging */
extern int gbl_sdebug;       /* enable sql operation debugging */
extern int gbl_debug_until;  /* enable who debugging */
extern int gbl_who;          /* enable who debugging */
extern int gbl_maxwthreads;  /* max write threads */
extern int gbl_maxthreads;   /* max number of threads allowed */
extern int gbl_maxqueue;     /* max number of requests to be queued up */
extern int gbl_thd_linger;   /* number of seconds for threads to linger */
extern char *gbl_myhostname; /* my hostname */
extern struct interned_string *gbl_myhostname_interned;
extern char *gbl_machine_class; /* my machine class */
extern struct in_addr gbl_myaddr;   /* my IPV4 address */
extern int gbl_mynodeid;     /* node number, for backwards compatibility */
extern pid_t gbl_mypid;      /* my pid */
extern int gbl_create_mode;  /* create files if no exists */
extern int gbl_logmemsize;   /* log memory size */
extern int gbl_fullrecovery; /* full recovery mode*/
extern int gbl_local_mode;   /* local mode, no siblings */
extern int gbl_report;       /* update rate to log */
extern int gbl_report_last;
extern long gbl_report_last_n;
extern long gbl_report_last_r;
extern int gbl_exit;           /* exit requested.*/
extern int gbl_maxretries;     /* max retries on deadlocks */
extern int gbl_maxblobretries; /* max retries on deadlocks */
extern int
    gbl_maxcontextskips; /* max records we will skip in a stable cursor */
extern int gbl_elect_time_secs; /* overrides elect time if > 0 */
extern int gbl_rtcpu_debug;    /* 1 to enable rtcpu debugging */
extern int gbl_longblk_trans_purge_interval; /* long transaction purge check
                                                interval. default 30 secs */
extern pthread_mutex_t gbl_sql_lock;
extern pthread_mutex_t
    gbl_sc_lock; /* used by schema change to protect globals */
extern int gbl_blob_maxage;
extern int gbl_blob_lose_debug;
extern int gbl_sqlflush_freq;
extern unsigned gbl_max_blob_cache_bytes;
extern int gbl_blob_vb;
extern long n_qtrap;
extern long n_qtrap_notcoherent;
extern long n_mtrap;
extern long n_mtrap_inline;
extern long n_bad_parm;
extern long n_bad_swapin;
extern long n_retries;
extern long n_missed;
extern long n_dbinfo;
extern history *reqhist;
extern int gbl_sbuftimeout;
extern int sqldbgflag;
extern int gbl_conv_flush_freq;       /* this is currently ignored */
extern int gbl_meta_lite;      /* used at init time to say we prefer lite */
extern int gbl_context_in_key; /* whether to drop find context in last
                                  key found (in dtastripe mode) */
extern int gbl_ready;          /* gets set just before waitft is called
                                  and never gets unset */
extern int gbl_queue_debug;
extern unsigned gbl_goose_add_rate;
extern unsigned gbl_goose_consume_rate;
extern int gbl_queue_sleeptime;
extern int gbl_reset_queue_cursor;
extern int gbl_readonly;
extern int gbl_readonly_sc;
extern int gbl_init_single_meta;
extern unsigned long long gbl_sc_genids[MAXDTASTRIPE];
extern int gbl_sc_usleep;
extern int gbl_sc_wrusleep;
extern int gbl_sc_last_writer_time;
extern int gbl_default_livesc;
extern int gbl_default_plannedsc;
extern int gbl_default_sc_scanmode;
extern int gbl_sc_abort;
extern int gbl_tranmode;
extern volatile uint32_t gbl_analyze_gen;
extern volatile int gbl_views_gen;
extern int gbl_sc_report_freq;
extern int gbl_thrman_trace;
extern int gbl_move_deadlk_max_attempt;
extern int gbl_lock_conflict_trace;
extern int gbl_inflate_log;
extern pthread_attr_t gbl_pthread_attr_detached;
extern int64_t gbl_nsql;
extern long long gbl_nsql_steps;

extern int64_t gbl_nnewsql;
extern int64_t gbl_nnewsql_ssl;
extern long long gbl_nnewsql_steps;

/* Legacy request metrics */
extern int64_t gbl_fastsql_execute_inline_params;
extern int64_t gbl_fastsql_set_isolation_level;
extern int64_t gbl_fastsql_set_timeout;
extern int64_t gbl_fastsql_set_info;
extern int64_t gbl_fastsql_execute_inline_params_tz;
extern int64_t gbl_fastsql_set_heartbeat;
extern int64_t gbl_fastsql_pragma;
extern int64_t gbl_fastsql_reset;
extern int64_t gbl_fastsql_execute_replaceable_params;
extern int64_t gbl_fastsql_set_sql_debug;
extern int64_t gbl_fastsql_grab_dbglog;
extern int64_t gbl_fastsql_set_user;
extern int64_t gbl_fastsql_set_password;
extern int64_t gbl_fastsql_set_endian;
extern int64_t gbl_fastsql_execute_replaceable_params_tz;
extern int64_t gbl_fastsql_get_effects;
extern int64_t gbl_fastsql_set_planner_effort;
extern int64_t gbl_fastsql_set_remote_access;
extern int64_t gbl_fastsql_osql_max_trans;
extern int64_t gbl_fastsql_set_datetime_precision;
extern int64_t gbl_fastsql_sslconn;
extern int64_t gbl_fastsql_execute_stop;

extern unsigned int gbl_masterrejects;

extern int gbl_selectv_rangechk;

extern int gbl_init_with_rowlocks;
extern int gbl_init_with_genid48;
extern int gbl_init_with_odh;
extern int gbl_init_with_queue_odh;
extern int gbl_init_with_queue_persistent_seq;
extern int gbl_init_with_ipu;
extern int gbl_init_with_instant_sc;
extern int gbl_init_with_compr;
extern int gbl_init_with_queue_compr;
extern int gbl_init_with_compr_blobs;
extern int gbl_init_with_bthash;

extern int gbl_sqlhistsz;
extern int gbl_replicate_local;
extern int gbl_replicate_local_concurrent;

extern int gbl_verify_abort;

extern int gbl_sort_nulls_correctly;

extern int gbl_master_changes;
extern int gbl_sc_commit_count;

extern int gbl_fix_validate_cstr;
extern int gbl_warn_validate_cstr;

extern int gbl_pushlogs_after_sc;
extern int gbl_prefault_verbose;
extern int gbl_check_client_tags;
extern int gbl_strict_dbl_quotes;

extern int gbl_max_tables_info;

extern int gbl_prefault_toblock_bcast;
extern int gbl_prefault_toblock_local;

extern int gbl_appsock_pooling;
extern struct thdpool *gbl_appsock_thdpool;
extern struct thdpool *gbl_osqlpfault_thdpool;
extern struct thdpool *gbl_udppfault_thdpool;

extern int gbl_consumer_rtcpu_check;
extern int gbl_node1rtcpuable;

extern int gbl_blockop_count_xrefs[BLOCK_MAXOPCODE];
extern const char *gbl_blockop_name_xrefs[NUM_BLOCKOP_OPCODES];
extern char appsock_unknown[];
extern char appsock_supported[];
extern char appsock_unknown_old[];
extern int gbl_serialise_sqlite3_open;

extern int gbl_rrenablecountchanges;

extern int gbl_debug_log_twophase;
extern int gbl_debug_log_twophase_transactions;

extern int gbl_chkpoint_alarm_time;

extern int gbl_incoherent_msg_freq;
extern int gbl_incoherent_alarm_time;
extern int gbl_max_incoherent_nodes;

extern int n_retries_transaction_active;
extern int n_retries_transaction_done;

extern int gbl_disable_deadlock_trace;
extern int gbl_enable_pageorder_trace;
extern int gbl_disable_overflow_page_trace;
extern int gbl_simulate_rowlock_deadlock_interval;

extern int gbl_max_columns_soft_limit;

extern int gbl_use_plan;

extern int gbl_num_record_converts;
extern int gbl_num_record_upgrades;

extern int gbl_enable_sql_stmt_caching;

extern int gbl_sql_pool_emergency_queuing_max;

extern int gbl_verify_rep_log_records;
extern int gbl_enable_osql_logging;
extern int gbl_enable_osql_longreq_logging;

extern int gbl_osql_verify_ext_chk;

extern int gbl_genid_cache;

extern int gbl_master_changed_oldfiles;
extern int gbl_extended_sql_debug_trace;
extern int gbl_use_sockpool_for_debug_logs;
extern int gbl_optimize_truncate_repdb;
extern int gbl_rep_process_txn_time;
extern int gbl_deadlock_policy_override;

extern int gbl_temptable_pool_capacity;
extern int gbl_memstat_freq;

extern int gbl_forbid_datetime_truncation;
extern int gbl_forbid_datetime_promotion;
extern int gbl_forbid_datetime_ms_us_s2s;

/* tunables for page compaction */
extern double gbl_pg_compact_thresh;
extern double gbl_pg_compact_target_ff;
extern int gbl_pg_compact_latency_ms;
extern int gbl_disable_backward_scan;
extern int gbl_compress_page_compact_log;
extern unsigned int gbl_max_num_compact_pages_per_txn;
extern char *gbl_dbdir;

extern double gbl_cpupercent;

extern int gbl_dohsql_disable;
extern int gbl_dohsql_verbose;
extern int gbl_dohast_disable;
extern int gbl_dohast_verbose;
extern int gbl_dohsql_max_queued_kb_highwm;
extern int gbl_dohsql_full_queue_poll_msec;
extern int gbl_dohsql_max_threads;
extern int gbl_dohsql_pool_thr_slack;
extern int gbl_dohsql_sc_max_threads;
extern int gbl_sockbplog;
extern int gbl_sockbplog_sockpool;

extern int gbl_logical_live_sc;

extern int gbl_test_io_errors;
extern uint64_t gbl_sc_headroom;
/* init routines */
int appsock_init(void);
int thd_init(void);
void thd_cleanup();
void sqlinit(void);
void sqlnet_init(void);
int sqlpool_init(void);
int schema_init(void);
int osqlpfthdpool_init(void);
int init_opcode_handlers();
void toblock_init(void);
int mach_class_init(void);

/* deinit routines */
int destroy_appsock(void);

/* comdb2 modules */
int process_command(struct dbenv *dbenv, char *line, int lline,
                    int st); /*handle message trap */
int process_sync_command(struct dbenv *dbenv, char *line, int lline,
                         int st); /*handle sync command*/
extern int comdb2_formdbkey(struct dbtable *db, int index, char *key, void *record,
                            int reclen); /* form key based on db
                                            and index.  This is
                                            only called for dbs
                                            that use SQL and
                                            schema tables */
const char *req2a(int opcode);
int a2req(const char *s);

/* request processing */
void appsock_handler_start(struct dbenv *dbenv, SBUF2 *sb, int is_admin);
void appsock_coalesce(struct dbenv *dbenv);
void close_appsock(SBUF2 *sb);
void thd_stats(void);
void thd_dbinfo2_stats(struct db_info2_stats *stats);
void thd_coalesce(struct dbenv *dbenv);
void unlock_swapin(void);
char *getorigin(struct ireq *iq);
void thd_dump(void);
int thd_queue_depth(void);

enum comdb2_queue_types {
    REQ_WAITFT = 0,
    REQ_SOCKET,
    REQ_OFFLOAD,
    REQ_SOCKREQUEST,
    REQ_PQREQUEST
};

int handle_buf_main(
    struct dbenv *dbenv, SBUF2 *sb, const uint8_t *p_buf,
    const uint8_t *p_buf_end, int debug, char *frommach, int frompid,
    char *fromtask, osql_sess_t *sorese, int qtype,
    void *data_hndl, // handle to data that can be used according to request
                     // type
    int luxref, unsigned long long rqid);
int handle_buf(struct dbenv *dbenv, uint8_t *p_buf, const uint8_t *p_buf_end,
               int debug, char *frommach); /* 040307dh: 64bits */
int handle_socket_long_transaction(struct dbenv *dbenv, SBUF2 *sb,
                                   uint8_t *p_buf, const uint8_t *p_buf_end,
                                   int debug, char *frommach, int frompid,
                                   char *fromtask);
int handle_buf_block_offload(struct dbenv *dbenv, uint8_t *p_buf,
                             const uint8_t *p_buf_end, int debug,
                             char *frommach, unsigned long long rqid);
void req_stats(struct dbtable *db);
void appsock_quick_stat(void);
void appsock_stat(void);
void appsock_get_dbinfo2_stats(uint32_t *n_appsock, uint32_t *n_sql);
void ixstats(struct dbenv *dbenv);
void curstats(struct dbenv *dbenv);

/* Not available - this is the initial state. */
#define REPLY_STATE_NA 0
/* Sent - set by a tag thread. */
#define REPLY_STATE_DONE 1
/* Discard - set by an appsock thread if the child tag thread has timed out. */
#define REPLY_STATE_DISCARD 2
struct buf_lock_t {
    pthread_mutex_t req_lock;
    pthread_cond_t wait_cond;
    int rc;
    int reply_state; /* See REPLY_STATE_* macros above */
    uint8_t *bigbuf;
    SBUF2 *sb;
};

#define MAX_BUFFER_SIZE 65536

int signal_buflock(struct buf_lock_t *p_slock);
int free_bigbuf(uint8_t *p_buf, struct buf_lock_t *p_slock);
int free_bigbuf_nosignal(uint8_t *p_buf);

/* request debugging */

void reqprintf(struct ireq *iq, char *format,
               ...); /* flush, and print current line */
void reqmoref(struct ireq *iq, char *format, ...); /* append to end of line */
void reqdumphex(struct ireq *iq, void *buf,
                int nb);             /* append hex dump to end of line */
void reqprintflush(struct ireq *iq); /* flush current line */
void reqpushprefixf(struct ireq *iq, const char *format, ...);
void reqpopprefixes(struct ireq *iq, int num);
const char *req2a(int opcode);
void reqerrstr(struct ireq *iq, int rc, char *format, ...);
void reqerrstrhdr(struct ireq *iq, char *format,
                  ...); /* keep an error header in a concatenated manner */
void reqerrstrclr(struct ireq *iq);
void reqerrstrhdrclr(struct ireq *iq); /* clear error header */

#define sc_client_error(s, fmt, ...)                                                                                   \
    do {                                                                                                               \
        reqerrstr((s)->iq, ERR_SC, fmt, ##__VA_ARGS__); /* cdb2sql */                                                  \
        sc_errf(s, fmt "\n", ##__VA_ARGS__);            /* comdb2sc */                                                 \
    } while (0)

/* internal request forwarding */
int ireq_forward_to_master(struct ireq *iq, int len);

int getkeyrecnums(const struct dbtable *db, int ixnum);
int getkeysize(const struct dbtable *db, int ixnum); /* get key size of db */
int getdatsize(const struct dbtable *db);            /* get data size of db*/
int getdefaultdatsize(const struct dbtable *db);
int getondiskclientdatsize(const struct dbtable *db);
int getclientdatsize(const struct dbtable *db, char *sname);

/*look up managed db's by number*/
struct dbtable *getdbbynum(int num);
/*look up managed db's by name*/
struct dbtable *get_dbtable_by_name(const char *name);
/* Lookup view by name */
struct dbview *get_view_by_name(const char *view_name);
/* Load all views from llmeta */
int llmeta_load_views(struct dbenv *, void *);
/*look up managed queue db's by name*/
struct dbtable *getqueuebyname(const char *name);
struct dbtable *getfdbbyrmtnameenv(struct dbenv *dbenv, const char *tblname);

int get_elect_time_microsecs(void); /* get election time in seconds */

/* glue */

/* open files and db. returns db backend handle */
int llmeta_set_tables(tran_type *tran, struct dbenv *dbenv);
int llmeta_dump_mapping_tran(void *tran, struct dbenv *dbenv);
int llmeta_dump_mapping(struct dbenv *dbenv);
int llmeta_dump_mapping_table_tran(void *tran, struct dbenv *dbenv,
                                   const char *table, int err);
int llmeta_dump_mapping_table(struct dbenv *dbenv, const char *table, int err);
int llmeta_load_lua_sfuncs();
int llmeta_load_lua_afuncs();
int backend_open(struct dbenv *dbenv);
int backend_open_tran(struct dbenv *dbenv, tran_type *tran, uint32_t flags);
int open_bdb_env(struct dbenv *dbenv);
int backend_close(struct dbenv *dbenv);
void backend_cleanup(struct dbenv *dbenv);
void backend_stat(struct dbenv *dbenv);
void backend_get_cachestats(struct dbenv *dbenv, int *cachekb, int *hits,
                            int *misses);

void backend_get_iostats(int *n_reads, int *l_reads, int *n_writes,
                         int *l_writes);

void *open_fstblk(struct dbenv *dbenv, int create_override);
int open_auxdbs(struct dbtable *db, int force_create);
void sb_printf(SBUF2 *sb, const char *fmt, ...);
void sb_errf(SBUF2 *sb, const char *fmt, ...);

void sc_status(struct dbenv *dbenv);
int close_all_dbs(void);
int open_all_dbs(void);
void apply_new_stripe_settings(int newdtastripe, int newblobstripe);

void sc_del_unused_files_check_progress(void);

/* update sync parameters*/
void logdelete_lock(const char *func, int line);
void logdelete_unlock(const char *func, int line);
void backend_update_sync(struct dbenv *dbenv);
void backend_sync_stat(struct dbenv *dbenv);

void init_fake_ireq_auxdb(struct dbenv *dbenv, struct ireq *iq, int auxdb);
void init_fake_ireq(struct dbenv *, struct ireq *);
void set_tran_verify_updateid(tran_type *tran);

/* long transaction routines */

int purge_expired_long_transactions(struct dbenv *dbenv);
int add_new_transaction_entry(struct dbenv *dbenv, void *entry);
void tran_dump(struct long_trn_stat *tstats);

/* transactional stuff */
int trans_start(struct ireq *, tran_type *parent, tran_type **out);
int trans_start_sc(struct ireq *, tran_type *parent, tran_type **out);
int trans_start_sc_fop(struct ireq *, tran_type **out);
int trans_start_sc_lowpri(struct ireq *, tran_type **out);
int trans_set_timestamp(bdb_state_type *bdb_state, tran_type *trans, int64_t timestamp);
int trans_get_timestamp(bdb_state_type *bdb_state, tran_type *trans, int64_t *timestamp);
int trans_start_set_retries(struct ireq *, tran_type *parent, tran_type **out,
                            uint32_t retries, uint32_t priority);
int trans_start_nonlogical(struct ireq *iq, void *parent_trans, tran_type **out_trans);
int trans_start_logical(struct ireq *, tran_type **out);
int trans_start_logical_sc(struct ireq *, tran_type **out);
int trans_start_logical_sc_with_force(struct ireq *, tran_type **out);
int is_rowlocks_transaction(tran_type *);
int rowlocks_check_commit_physical(bdb_state_type *, tran_type *,
                                   int blockop_count);
tran_type *trans_start_readcommitted(struct ireq *, int trak);
tran_type *trans_start_modsnap(struct ireq *, int trak);
tran_type *trans_start_serializable(struct ireq *, int trak, int epoch,
                                    int file, int offset, int *error,
                                    int is_ha_retry);
tran_type *trans_start_snapisol(struct ireq *, int trak, int epoch, int file, int offset, int *error, int is_ha_retry);
tran_type *trans_start_socksql(struct ireq *, int trak);
int trans_commit(struct ireq *iq, void *trans, char *source_host);
int trans_commit_nowait(struct ireq *iq, void *trans, char *source_host);
int trans_commit_seqnum(struct ireq *iq, void *trans, db_seqnum_type *seqnum);
int trans_commit_adaptive(struct ireq *iq, void *trans, char *source_host);
int trans_commit_logical(struct ireq *iq, void *trans, char *source_host,
                         int timeoutms, int adaptive, void *blkseq, int blklen,
                         void *blkkey, int blkkeylen);
int trans_abort(struct ireq *iq, void *trans);
int trans_abort_priority(struct ireq *iq, void *trans, int *priority);
int trans_abort_logical(struct ireq *iq, void *trans, void *blkseq, int blklen,
                        void *blkkey, int blkkeylen);
int trans_discard_prepared(struct ireq *iq, void *trans);
int trans_wait_for_seqnum(struct ireq *iq, char *source_host,
                          db_seqnum_type *ss);
int trans_wait_for_last_seqnum(struct ireq *iq, char *source_host);

/* find context for pseudo-stable cursors */
int get_context(struct ireq *iq, unsigned long long *context);
int cmp_context(struct ireq *iq, unsigned long long genid,
                unsigned long long context);

/*index routines*/
int ix_isnullk(const dbtable *db_table, void *key, int ixnum);
int ix_addk(struct ireq *iq, void *trans, void *key, int ixnum,
            unsigned long long genid, int rrn, void *dta, int dtalen, int isnull);
int ix_addk_auxdb(int auxdb, struct ireq *iq, void *trans, void *key, int ixnum,
                  unsigned long long genid, int rrn, void *dta, int dtalen, int isnull);
int ix_upd_key(struct ireq *iq, void *trans, void *key, int keylen, int ixnum,
               unsigned long long genid, unsigned long long oldgenid, void *dta,
               int dtalen, int isnull);

int ix_delk(struct ireq *iq, void *trans, void *key, int ixnum, int rrn,
            unsigned long long genid, int isnull);
int ix_delk_auxdb(int auxdb, struct ireq *iq, void *trans, void *key, int ixnum,
                  int rrn, unsigned long long genid, int isnull);

enum {
    IX_FIND_IGNORE_INCOHERENT = 1
    /* 2, 4, 8, etc */
};
int ix_find_flags(struct ireq *iq, void *trans, int ixnum, void *key,
                  int keylen, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, int flags);

int ix_find(struct ireq *iq, int ixnum, void *key, int keylen, void *fndkey,
            int *fndrrn, unsigned long long *genid, void *fnddta, int *fndlen,
            int maxlen);
int ix_find_nl_ser(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen,
                   bdb_cursor_ser_t *cur_ser);
int ix_find_nl_ser_flags(struct ireq *iq, int ixnum, void *key, int keylen,
                         void *fndkey, int *fndrrn, unsigned long long *genid,
                         void *fnddta, int *fndlen, int maxlen,
                         bdb_cursor_ser_t *cur_ser, int flags);
int ix_find_trans(struct ireq *iq, void *trans, int ixnum, void *key,
                  int keylen, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen);
int ix_find_blobs(struct ireq *iq, int ixnum, void *key, int keylen,
                  void *fndkey, int *fndrrn, unsigned long long *genid,
                  void *fnddta, int *fndlen, int maxlen, int numblobs,
                  int *blobnums, size_t *blobsizes, size_t *bloboffs,
                  void **blobptrs, int *retries);
int ix_find_auxdb(int auxdb, struct ireq *iq, int ixnum, void *key, int keylen,
                  void *fndkey, int *fndrrn, unsigned long long *genid,
                  void *fnddta, int *fndlen, int maxlen);

int ix_next(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
            int lastrrn, unsigned long long lastgenid, void *fndkey,
            int *fndrrn, unsigned long long *genid, void *fnddta, int *fndlen,
            int maxlen, unsigned long long context);
int ix_next_nl_ser(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *last, int lastrrn, unsigned long long lastgenid,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen,
                   unsigned long long context, bdb_cursor_ser_t *cur_ser);
int ix_next_nl_ser_flags(struct ireq *iq, int ixnum, void *key, int keylen,
                         void *last, int lastrrn, unsigned long long lastgenid,
                         void *fndkey, int *fndrrn, unsigned long long *genid,
                         void *fnddta, int *fndlen, int maxlen,
                         unsigned long long context, bdb_cursor_ser_t *cur_ser,
                         int flags);
int ix_next_nl(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
               int lastrrn, unsigned long long lastgenid, void *fndkey,
               int *fndrrn, unsigned long long *genid, void *fnddta,
               int *fndlen, int maxlen, unsigned long long context);
int ix_next_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                  void *key, int keylen, void *last, int lastrrn,
                  unsigned long long lastgenid, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, unsigned long long context);
int ix_next_blobs(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                  int lastrrn, unsigned long long lastgenid, void *fndkey,
                  int *fndrrn, unsigned long long *genid, void *fnddta,
                  int *fndlen, int maxlen, int numblobs, int *blobnums,
                  size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                  int *retries, unsigned long long context);
int ix_next_blobs_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                        void *key, int keylen, void *last, int lastrrn,
                        unsigned long long lastgenid, void *fndkey, int *fndrrn,
                        unsigned long long *genid, void *fnddta, int *fndlen,
                        int maxlen, int numblobs, int *blobnums,
                        size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                        int *retries, unsigned long long context);
int ix_next_trans(struct ireq *iq, void *trans, int ixnum, void *key,
                  int keylen, void *last, int lastrrn,
                  unsigned long long lastgenid, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, unsigned long long context);

int ix_prev(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
            int lastrrn, unsigned long long lastgenid, void *fndkey,
            int *fndrrn, unsigned long long *genid, void *fnddta, int *fndlen,
            int maxlen, unsigned long long context);
int ix_prev_nl_ser(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *last, int lastrrn, unsigned long long lastgenid,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen,
                   unsigned long long context, bdb_cursor_ser_t *cur_ser);
int ix_prev_nl(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
               int lastrrn, unsigned long long lastgenid, void *fndkey,
               int *fndrrn, unsigned long long *genid, void *fnddta,
               int *fndlen, int maxlen, unsigned long long context);
int ix_prev_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                  void *key, int keylen, void *last, int lastrrn,
                  unsigned long long lastgenid, void *fndkey, int *fndrrn,
                  unsigned long long *genid, void *fnddta, int *fndlen,
                  int maxlen, unsigned long long context);
int ix_prev_blobs(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                  int lastrrn, unsigned long long lastgenid, void *fndkey,
                  int *fndrrn, unsigned long long *genid, void *fnddta,
                  int *fndlen, int maxlen, int numblobs, int *blobnums,
                  size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                  int *retries, unsigned long long context);
int ix_prev_blobs_auxdb(int auxdb, int lookahead, struct ireq *iq, int ixnum,
                        void *key, int keylen, void *last, int lastrrn,
                        unsigned long long lastgenid, void *fndkey, int *fndrrn,
                        unsigned long long *genid, void *fnddta, int *fndlen,
                        int maxlen, int numblobs, int *blobnums,
                        size_t *blobsizes, size_t *bloboffs, void **blobptrs,
                        int *retries, unsigned long long context);

int get_next_genids(struct ireq *iq, int ixnum, void *key, int keylen,
                    unsigned long long *genids, int maxgenids,
                    int *num_genids_gotten);

int ix_find_auxdb_by_rrn_and_genid(int auxdb, struct ireq *iq, int rrn,
                                   unsigned long long genid, void *fnddta,
                                   int *fndlen, int maxlen);
int ix_find_by_rrn_and_genid(struct ireq *iq, int rrn, unsigned long long genid,
                             void *fnddta, int *fndlen, int maxlen);
int ix_find_by_genid_origin(struct ireq *iq, unsigned long long genid,
                            void *fnddta, int *fndlen, int maxlen);
int ix_find_by_rrn_and_genid_prefault(struct ireq *iq, int rrn,
                                      unsigned long long genid, void *fnddta,
                                      int *fndlen, int maxlen);
int ix_find_by_rrn_and_genid_tran(struct ireq *iq, int rrn,
                                  unsigned long long genid, void *fnddta,
                                  int *fndlen, int maxlen, void *trans);
int ix_load_for_write_by_genid_tran(struct ireq *iq, int rrn,
        unsigned long long genid, void *fnddta,
        int *fndlen, int maxlen, void *trans);
int ix_find_ver_by_rrn_and_genid_tran(struct ireq *iq, int rrn,
                                      unsigned long long genid, void *fnddta,
                                      int *fndlen, int maxlen, void *trans,
                                      int *version);
int ix_find_by_rrn_and_genid_dirty(struct ireq *iq, int rrn,
                                   unsigned long long genid, void *fnddta,
                                   int *fndlen, int maxlen);
int ix_find_dirty(struct ireq *iq, int ixnum, void *key, int keylen,
                  void *fndkey, int *fndrrn, unsigned long long *genid,
                  void *fnddta, int *fndlen, int maxlen);
int ix_find_prefault(struct ireq *iq, int ixnum, void *key, int keylen,
                     void *fndkey, int *fndrrn, unsigned long long *genid,
                     void *fnddta, int *fndlen, int maxlen);
int ix_find_nodatacopy(struct ireq *iq, int ixnum, void *key, int keylen,
                       void *fndkey, int *fndrrn, unsigned long long *genid,
                       void *fnddta, int *fndlen, int maxlen);
int ix_fetch_last_key_tran(struct ireq *iq, void *tran, int write, int ixnum,
                           int keylen, void *fndkey, int *fndlen);

int ix_find_auxdb_blobs_by_rrn_and_genid(int auxdb, struct ireq *iq, int rrn,
                                         unsigned long long genid, int numblobs,
                                         int *blobnums, size_t *blobsizes,
                                         size_t *bloboffs, void **blobptrs);
int ix_find_blobs_by_rrn_and_genid(struct ireq *iq, int rrn,
                                   unsigned long long genid, int numblobs,
                                   int *blobnums, size_t *blobsizes,
                                   size_t *bloboffs, void **blobptrs);
int ix_find_auxdb_blobs_by_rrn_and_genid_tran(
    int auxdb, struct ireq *iq, void *trans, int rrn, unsigned long long genid,
    int numblobs, int *blobnums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs);
int ix_find_blobs_by_rrn_and_genid_tran(struct ireq *iq, void *trans, int rrn,
                                        unsigned long long genid, int numblobs,
                                        int *blobnums, size_t *blobsizes,
                                        size_t *bloboffs, void **blobptrs);

int ix_find_by_primkey_tran(struct ireq *iq, void *key, int keylen,
                            void *fndkey, int *fndrrn,
                            unsigned long long *genid, void *fnddta,
                            int *fndlen, int maxlen, void *trans);
int ix_find_auxdb_by_primkey_tran(int auxdb, struct ireq *iq, void *key,
                                  int keylen, void *fndkey, int *fndrrn,
                                  unsigned long long *genid, void *fnddta,
                                  int *fndlen, int maxlen, void *trans);

int ix_find_by_key_tran(struct ireq *iq, void *key, int keylen, int index,
                        void *fndkey, int *fndrrn, unsigned long long *genid,
                        void *fnddta, int *fndlen, int maxlen, void *trans);
int ix_find_auxdb_by_key_tran(int auxdb, struct ireq *iq, void *key, int keylen,
                              int index, void *fndkey, int *fndrrn,
                              unsigned long long *genid, void *fnddta,
                              int *fndlen, int maxlen, void *trans);

/* This is pretty much a straight through wrapper for the bdb range delete. */
typedef int (*comdb2_formkey_callback_t)(void *, size_t, void *, size_t, int,
                                         void *);
typedef int (*comdb2_pre_delete_callback_t)(void *, size_t, int,
                                            unsigned long long, void *);
typedef int (*comdb2_post_delete_callback_t)(void *, size_t, int,
                                             unsigned long long, void *);
int ix_rng_del(struct ireq *iq, void *trans, size_t dtalen, int index,
               const void *start_key, const void *end_key, size_t keylength,
               comdb2_formkey_callback_t formkey_callback,
               comdb2_pre_delete_callback_t pre_callback,
               comdb2_post_delete_callback_t post_callback, void *userptr,
               int *count_deleted, int *count_not_deleted, int max_records,
               int max_time_ms);
int ix_rng_del_auxdb(int auxdb, struct ireq *iq, void *trans, size_t dtalen,
                     int index, const void *start_key, const void *end_key,
                     size_t keylength,
                     comdb2_formkey_callback_t formkey_callback,
                     comdb2_pre_delete_callback_t pre_callback,
                     comdb2_post_delete_callback_t post_callback, void *userptr,
                     int *count_deleted, int *count_not_deleted,
                     int max_records, int max_time_ms);

int dat_upgrade(struct ireq *iq, void *trans, void *newdta, int newlen,
                unsigned long long vgenid);

int dat_upv(struct ireq *iq, void *trans, int vptr, void *vdta, int vlen,
            unsigned long long vgenid, void *newdta, int newlen, int rrn,
            unsigned long long *genid, int verifydta, int modnum);
int dat_upv_sc(struct ireq *iq, void *trans, int vptr, void *vdta, int vlen,
               unsigned long long vgenid, void *newdta, int newlen, int rrn,
               unsigned long long *genid, int verifydta, int modnum);
int dat_upv_auxdb(int auxdb, struct ireq *iq, void *trans, int vptr, void *vdta,
                  int vlen, unsigned long long vgenid, void *newdta, int newlen,
                  int rrn, unsigned long long *genid, int verifydta, int modnum,
                  int use_new_genid);
int dat_upv_noblobs(struct ireq *iq, void *trans, int vptr, void *vdta,
                    int vlen, unsigned long long vgenid, void *newdta,
                    int newlen, int rrn, unsigned long long *genid,
                    int verifydta);

int blob_upv_auxdb(int auxdb, struct ireq *iq, void *trans, int vptr,
                   unsigned long long oldgenid, void *newdta, int newlen,
                   int blobno, int rrn, unsigned long long newgenid,
                   int odhready);
int blob_no_upd_auxdb(int auxdb, struct ireq *iq, void *trans, int rrn,
                      unsigned long long oldgenid, unsigned long long newgenid,
                      int blobmap);
int blob_upv(struct ireq *iq, void *trans, int vptr,
             unsigned long long oldgenid, void *newdta, int newlen, int blobno,
             int rrn, unsigned long long newgenid, int odhready);
int blob_no_upd(struct ireq *iq, void *trans, int rrn,
                unsigned long long oldgenid, unsigned long long newgenid,
                int blobmap);
int dat_get_active_stripe(struct ireq *iq);
int dat_add(struct ireq *iq, void *trans, void *data, int datalen,
            unsigned long long *genid, int *out_rrn);
int dat_add_auxdb(int auxdb, struct ireq *iq, void *trans, void *data,
                  int datalen, unsigned long long *genid, int *out_rrn);
int dat_set(struct ireq *iq, void *trans, void *data, size_t length, int rrn,
            unsigned long long genid);

int dat_del(struct ireq *iq, void *trans, int rrn, unsigned long long genid);
int dat_del_auxdb(int auxdb, struct ireq *iq, void *trans, int rrn,
                  unsigned long long genid, int delblobs);

int dat_numrrns(struct ireq *iq, int *out_numrrns);
int dat_highrrn(struct ireq *iq, int *out_highrrn);

int blob_add(struct ireq *iq, void *trans, int blobno, void *data,
             size_t length, int rrn, unsigned long long genid, int odhready);

int blob_del(struct ireq *iq, void *trans, int rrn, unsigned long long genid,
             int blobno);
int blob_del_auxdb(int auxdb, struct ireq *iq, void *trans, int rrn,
                   unsigned long long genid, int blobno);

int blob_upd_genid(struct ireq *iq, void *trans, int blobno, int rrn,
                   unsigned long long oldgenid, unsigned long long newgenid);

int metadata_get(void *handle, int rrn, int attr, void *dta, int datalen,
                 int *outlen, unsigned long long *genid, int *bdberr);
int metadata_put(void *handle, int rrn, int attr, void *data, int datalen,
                 void *tran, int *bdberr);

int ix_next_rnum(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                 int lastrrn, unsigned long long lastgenid, void *fndkey,
                 int *fndrrn, unsigned long long *genid, void *fnddta,
                 int *fndlen, int *recnum, int maxlen);
int ix_prev_rnum(struct ireq *iq, int ixnum, void *key, int keylen, void *last,
                 int lastrrn, unsigned long long lastgenid, void *fndkey,
                 int *fndrrn, unsigned long long *genid, void *fnddta,
                 int *fndlen, int *recnum, int maxlen);

int dtas_next(struct ireq *iq, const unsigned long long *genid_vector,
              unsigned long long *genid, int *stripe, int stay_in_stripe,
              void *dta, void *trans, int dtalen, int *reqdtalen, int *ver);
int dtas_next_pageorder(struct ireq *iq, const unsigned long long *genid_vector,
                        unsigned long long *genid, int *stripe,
                        int stay_in_stripe, void *dta, void *trans, int dtalen,
                        int *reqdtalen, int *ver);

struct dbtable *find_table(const char *table);
int bt_hash_table(char *table, int szkb);
int del_bt_hash_table(char *table);
int stat_bt_hash_table(char *table);
int stat_bt_hash_table_reset(char *table);
int fastinit_table(struct dbenv *dbenvin, char *table);

void cleanup_newdb(struct dbtable *);
struct dbtable *newqdb(struct dbenv *env, const char *name, int avgsz, int pagesize,
                  int isqueuedb);
int add_queue_to_environment(char *table, int avgitemsz, int pagesize);
void stop_threads(struct dbenv *env);
void resume_threads(struct dbenv *env);
int add_dbtable_to_thedb_dbs(dbtable *table);
void rem_dbtable_from_thedb_dbs(dbtable *table);
void re_add_dbtable_to_thedb_dbs(dbtable *table);
void hash_sqlalias_db(dbtable *db, const char *newname);
int rename_db(struct dbtable *db, const char *newname);
int ix_find_rnum_by_recnum(struct ireq *iq, int recnum_in, int ixnum,
                           void *fndkey, int *fndrrn, unsigned long long *genid,
                           void *fnddta, int *fndlen, int *recnum, int maxlen);
int get_schema_version(const char *table);
int put_schema_version(const char *table, void *tran, int version);

int put_db_odh(struct dbtable *db, tran_type *, int odh);
int get_db_odh(struct dbtable *db, int *odh);
int get_db_odh_tran(struct dbtable *, int *odh, tran_type *);
int put_db_queue_odh(struct dbtable *db, tran_type *, int odh);
int get_db_queue_odh(struct dbtable *db, int *odh);
int get_db_queue_odh_tran(struct dbtable *, int *odh, tran_type *);
int put_db_queue_compress(struct dbtable *db, tran_type *, int odh);
int get_db_queue_compress(struct dbtable *db, int *odh);
int get_db_queue_compress_tran(struct dbtable *, int *odh, tran_type *);
int put_db_queue_persistent_seq(struct dbtable *db, tran_type *, int persist);
int get_db_queue_persistent_seq(struct dbtable *db, int *persist);
int get_db_queue_persistent_seq_tran(struct dbtable *, int *persist,
                                     tran_type *);
int put_db_queue_sequence(struct dbtable *db, tran_type *, long long seq);
int get_db_queue_sequence(struct dbtable *db, long long *seq);
int get_db_queue_sequence_tran(struct dbtable *, long long *seq, tran_type *);
int put_db_compress(struct dbtable *db, tran_type *, int compress);
int get_db_compress(struct dbtable *db, int *compress);
int get_db_compress_tran(struct dbtable *, int *compress, tran_type *);
int put_db_compress_blobs(struct dbtable *db, tran_type *, int compress_blobs);
int get_db_compress_blobs(struct dbtable *db, int *compress_blobs);
int get_db_compress_blobs_tran(struct dbtable *, int *compress_blobs, tran_type *);
int put_db_inplace_updates(struct dbtable *db, tran_type *, int ipupdates);
int get_db_inplace_updates(struct dbtable *db, int *ipupdates);
int get_db_inplace_updates_tran(struct dbtable *, int *ipupdates, tran_type *);
int put_db_datacopy_odh(struct dbtable *db, tran_type *, int cdc);
int get_db_datacopy_odh(struct dbtable *db, int *cdc);
int get_db_datacopy_odh_tran(struct dbtable *, int *cdc, tran_type *);
int put_db_bthash(struct dbtable *db, tran_type *, int bthashsz);
int get_db_bthash(struct dbtable *db, int *bthashsz);
int get_db_bthash_tran(struct dbtable *, int *bthashsz, tran_type *);
int put_db_instant_schema_change(struct dbtable *db, tran_type *tran, int isc);
int get_db_instant_schema_change(struct dbtable *db, int *isc);
int get_db_instant_schema_change_tran(struct dbtable *, int *isc, tran_type *tran);

int set_meta_odh_flags(struct dbtable *db, int odh, int compress, int compress_blobs,
                       int ipupates);
int set_meta_odh_flags_tran(struct dbtable *db, tran_type *tran, int odh,
                            int compress, int compress_blobs, int ipupdates);

int get_csc2_version(const char *table);
int get_csc2_version_tran(const char *table, tran_type *);
int get_csc2_file(const char *table, int version, char **text, int *len);
int get_csc2_file_tran(const char *table, int version, char **text, int *len,
                       tran_type *);
int put_csc2_file(const char *table, void *tran, int version, const char *text);
int put_csc2_stuff(struct dbtable *db, void *trans, void *stuff, size_t lenstuff);
int put_blobstripe_genid(struct dbtable *db, void *tran, unsigned long long genid);
int get_blobstripe_genid(struct dbtable *db, unsigned long long *genid);
int get_blobstripe_genid_tran(struct dbtable *db, unsigned long long *genid,
                              tran_type *tran);

int load_new_table_schema_file(struct dbenv *dbenv, const char *table,
                               const char *csc2file);
int load_new_table_schema_file_trans(void *tran, struct dbenv *dbenv,
                                     const char *table, const char *csc2file);
int load_new_table_schema_tran(struct dbenv *dbenv, tran_type *tran,
                               const char *table, const char *csc2_text);
int load_new_table_schema(struct dbenv *dbenv, const char *table,
                          const char *csc2_text);
void fix_blobstripe_genids(tran_type *tran);
int dump_all_csc2_to_disk();
int dump_table_csc2_to_disk_fname(struct dbtable *db, const char *csc2_fname);
int dump_table_csc2_to_disk(const char *table);
int get_csc2_fname(const struct dbtable *db, const char *dir, char *fname,
                   size_t fname_len);
int get_generic_csc2_fname(const struct dbtable *db, char *fname, size_t fname_len);

void flush_db(void);
void dump_cache(const char *file, int max_pages);
void load_cache(const char *file);
void load_cache_default(void);
void dump_cache_default(void);
int compare_all_tags(const char *table, FILE *out);
int restore_constraint_pointers(struct dbtable *db, struct dbtable *newdb);
int backout_constraint_pointers(struct dbtable *db, struct dbtable *newdb);
int populate_reverse_constraints(struct dbtable *db);
void init_reverse_constraints(struct dbtable *db);
int add_reverse_constraint(struct dbtable *db, constraint_t *cnstrt);
int delete_reverse_constraint(struct dbtable *db, size_t idx);
int has_index_changed(struct dbtable *db, char *keynm, int ct_check, int newkey,
                      FILE *out, int accept_type_change);
int resume_schema_change(void);

void debug_trap(char *line, int lline);
int ix_find_last_dup_rnum_kl(struct ireq *iq, int ixnum, void *key, int keylen,
                             void *fndkey, int *fndrrn,
                             unsigned long long *genid, void *fnddta,
                             int *fndlen, int *recnum, int maxlen);
int ix_find_rnum_kl(struct ireq *iq, int ixnum, void *key, int keylen,
                    void *fndkey, int *fndrrn, unsigned long long *genid,
                    void *fnddta, int *fndlen, int *recnum, int maxlen,
                    int comdbg_flags);
int ix_find_rnum_by_recnum_kl(struct ireq *iq, int recnum_in, int ixnum,
                              void *fndkey, int *fndrrn,
                              unsigned long long *genid, void *fnddta,
                              int *fndlen, int *recnum, int maxlen);
int ix_next_rnum_kl(struct ireq *iq, int ixnum, void *key, int keylen,
                    void *last, int lastrrn, unsigned long long lastgenid,
                    void *fndkey, int *fndrrn, unsigned long long *genid,
                    void *fnddta, int *fndlen, int *recnum, int maxlen,
                    int comdbg_flags);
int ix_find_rnum(struct ireq *iq, int ixnum, void *key, int keylen,
                 void *fndkey, int *fndrrn, unsigned long long *genid,
                 void *fnddta, int *fndlen, int *recnum, int maxlen);
void purgerrns(struct dbtable *db);

/* broadcast messages to other nodes */
int send_to_all_nodes(void *dta, int len, int type, int waittime);
int broadcast_add_new_queue(char *table, int avgitemsz);
int broadcast_add_consumer(const char *queuename, int consumern,
                           const char *method);
int broadcast_procedure_op(int op, const char *name, const char *param);
int broadcast_sc_start(const char *table, uint64_t seed, uint32_t host,
                       time_t t);
int broadcast_sc_ok(void);
int broadcast_flush_all(void);

int send_forgetmenot(void);

/* glue routines for "lite" databases */
int is_auxdb_lite(int auxdb, struct ireq *iq);
int lite_add_auxdb(int auxdb, struct ireq *iq, void *trans, void *data,
                   int datalen, void *key);
int lite_find_exact_auxdb_tran(int auxdb, struct ireq *iq, tran_type *tran,
                               void *key, void *fnddta, int *fndlen,
                               int maxlen);
int lite_find_exact_auxdb(int auxdb, struct ireq *iq, void *key, void *fnddta,
                          int *fndlen, int maxlen);
int lite_find_exact_var_auxdb_tran(int auxdb, struct ireq *iq, tran_type *tran,
                                   void *key, void **fnddta, int *fndlen);
int lite_find_exact_var_auxdb(int auxdb, struct ireq *iq, void *key,
                              void **fnddta, int *fndlen);
int lite_del_auxdb(int auxdb, struct ireq *iq, void *trans, void *key);
int lite_get_keys_auxdb(int auxdb, struct ireq *iq, void *firstkey,
                        void *fndkeys, int maxfnd, int *numfnd);

/* queue databases */
struct bdb_queue_found;
struct bdb_queue_cursor;
int dbq_add(struct ireq *iq, void *trans, const void *dta, size_t dtalen);
int dbq_consume(struct ireq *iq, void *trans, int consumer,
                const struct bdb_queue_found *fnd);
int dbq_consume_genid(struct ireq *, void *trans, int consumer, const genid_t);
int dbq_get(struct ireq *iq, int consumer, const struct bdb_queue_cursor *prev, struct bdb_queue_found **fnddta,
            size_t *fnddtalen, size_t *fnddtaoff, struct bdb_queue_cursor *fnd, long long *seq, uint32_t lockid);
void dbq_get_item_info(const struct bdb_queue_found *fnd, size_t *dtaoff, size_t *dtalen);
unsigned long long dbq_item_genid(const struct bdb_queue_found *dta);
typedef int (*dbq_walk_callback_t)(int consumern, size_t item_length,
                                   unsigned int epoch, void *userptr);
typedef int (*dbq_stats_callback_t)(int consumern, size_t item_length,
                                    unsigned int epoch, unsigned int depth,
                                    void *userptr);

int dbq_walk(struct ireq *iq, int flags, dbq_walk_callback_t callback,
             tran_type *tran, void *userptr);
int dbq_odh_stats(struct ireq *iq, dbq_stats_callback_t callback,
                  tran_type *tran, void *userptr);
int dbq_dump(struct dbtable *db, FILE *out);
int fix_consumers_with_bdblib(struct dbenv *dbenv);
int dbq_add_goose(struct ireq *iq, void *trans);
int dbq_check_goose(struct ireq *iq, void *trans);
int dbq_consume_goose(struct ireq *iq, void *trans);

/* sql stuff */
int create_sqlmaster_records(void *tran);
int create_sqlmaster_records_flags(void *tran, uint32_t flags);
void form_new_style_name(char *namebuf, int len, struct schema *schema,
                         const char *csctag, const char *dbname);

typedef struct master_entry master_entry_t;
int get_copy_rootpages_custom(struct sql_thread *thd, master_entry_t *ents,
                              int nents);
int get_copy_rootpages_nolock(struct sql_thread *thd);
int get_copy_rootpages(struct sql_thread *thd);
int get_copy_rootpages_selectfire(struct sql_thread *thd, int nnames,
                                  const char **names,
                                  struct master_entry **oldentries,
                                  int *oldnentries, int lock);
void restore_old_rootpages(struct sql_thread *thd, master_entry_t *ents,
                           int nents);
int create_datacopy_arrays(void);
int create_datacopy_array(struct dbtable *db);
master_entry_t *create_master_entry_array(struct dbtable **dbs, int num_dbs,
                                          hash_t *view_hash, int *nents);
void cleanup_sqlite_master();
void create_sqlite_master();
int destroy_sqlite_master(master_entry_t *, int);
int sql_syntax_check(struct ireq *iq, struct dbtable *db);
void sql_dump_running_statements(void);
char *stradd(char **s1, char *s2, int freeit);
void dbgtrace(int, char *, ...);

int get_sqlite_entry_size(struct sql_thread *thd, int n);
void *get_sqlite_entry(struct sql_thread *thd, int n);
struct dbtable *get_sqlite_db(struct sql_thread *thd, int iTable, int *ixnum);

int schema_var_size(struct schema *sc);
int handle_ireq(struct ireq *iq);
int toblock(struct ireq *iq);
int to_sorese_init(struct ireq *iq);
int to_sorese(struct ireq *iq);
void count_table_in_thread(const char *table);
int findkl_enable_blob_verify(void);
void sltdbt_get_stats(int *n_reqs, int *l_reqs);
void dbghexdump(int flag, void *memp, size_t len);
void hash_set_hashfunc(hash_t *h, hashfunc_t hashfunc);
void hash_set_cmpfunc(hash_t *h, cmpfunc_t cmpfunc);

enum mach_class get_my_mach_class(void);
enum mach_class get_mach_class(const char *host);
const char *get_my_mach_class_str(void);
const char *get_mach_class_str(char *host);
int allow_write_from_remote(const char *host);
int allow_cluster_from_remote(const char *host);
int allow_broadcast_to_remote(const char *host);
int process_allow_command(char *line, int lline);

/* blob caching to support find requests */
int gather_blob_data(struct ireq *iq, const char *tag, blob_status_t *b,
                     const char *to_tag);
int gather_blob_data_byname(struct dbtable *table, const char *tag,
                            blob_status_t *b, struct schema *pd);
int check_one_blob_consistency(struct ireq *iq, struct dbtable *table,
                               const char *tag, blob_status_t *b, void *record,
                               int blob_index, int cblob, struct schema *pd);
int check_blob_consistency(struct ireq *iq, struct dbtable *table, const char *tag,
                           blob_status_t *b, const void *record);
int check_and_repair_blob_consistency(struct ireq *iq, struct dbtable *table,
                                      const char *tag, blob_status_t *b,
                                      const void *record);
void free_blob_status_data(blob_status_t *b);
void free_blob_status_data_noreset(blob_status_t *b);
int cache_blob_data(struct ireq *iq, int rrn, unsigned long long genid,
                    const char *table, const char *tag, unsigned *extra1,
                    unsigned *extra2, int numblobs, size_t *bloblens,
                    size_t *bloboffs, void **blobptrs, size_t total_length);
int init_blob_cache(void);
void blob_print_stats(void);
void purge_old_cached_blobs(void);

void commit_schemas(const char *tblname);
struct schema *new_dynamic_schema(const char *s, int len, int trace);
void free_dynamic_schema(const char *table, struct schema *dsc);
int getdefaultkeysize(const struct dbtable *tbl, int ixnum);
int getdefaultdatsize(const struct dbtable *tbl);
int update_sqlite_stats(struct ireq *iq, void *trans, void *dta);
void *do_verify(void *);
void dump_tagged_buf(struct dbtable *table, const char *tag,
                     const unsigned char *buf);
int ix_find_by_rrn_and_genid_get_curgenid(struct ireq *iq, int rrn,
                                          unsigned long long genid,
                                          unsigned long long *outgenid,
                                          void *fnddta, int *fndlen,
                                          int maxlen);
int ix_find_last_dup_rnum(struct ireq *iq, int ixnum, void *key, int keylen,
                          void *fndkey, int *fndrrn, unsigned long long *genid,
                          void *fnddta, int *fndlen, int *recnum, int maxlen);
void dump_record_by_rrn_genid(struct dbtable *db, int rrn, unsigned long long genid);
void upgrade_record_by_genid(struct dbtable *db, unsigned long long genid);
void backend_thread_event(struct dbenv *dbenv, int event);
void backend_cmd(struct dbenv *dbenv, char *line, int lline, int st);
uint64_t calc_table_size(struct dbtable *db, int skip_blobs);
uint64_t calc_table_size_tran(tran_type *tran, struct dbtable *db, int skip_blobs);

enum { WHOLE_BUFFER = -1 };

void diagnostics_dump_rrn(struct dbtable *tbl, int rrn);
void diagnostics_dump_dta(struct dbtable *db, int dtanum);

/* queue stuff */
void dbqueuedb_coalesce(struct dbenv *dbenv);
void dbqueuedb_admin(struct dbenv *dbenv);
int dbqueuedb_add_consumer(struct dbtable *db, int consumer, const char *method,
                         int noremove);
int consumer_change(const char *queuename, int consumern, const char *method);
int dbqueuedb_wake_all_consumers(struct dbtable *db, int force);
int dbqueuedb_wake_all_consumers_all_queues(struct dbenv *dbenv, int force);
int dbqueuedb_stop_consumers(struct dbtable *db);
int dbqueuedb_restart_consumers(struct dbtable *db);
int dbqueuedb_check_consumer(const char *method);
int dbqueuedb_get_name(struct dbtable *db, char **spname);
int dbqueuedb_get_stats(struct dbtable *db, struct consumer_stat *stats, uint32_t lockid);

/* Resource manager */
void initresourceman(const char *newlrlname);
char *getdbrelpath(const char *relpath);
void addresource(const char *name, const char *filepath);
const char *getresourcepath(const char *name);
void dumpresources(void);
void cleanresources(void);

/* for the hackery that gets findnext passing "lastgenid" */
void split_genid(unsigned long long genid, unsigned int *rrn1,
                 unsigned int *rrn2);
unsigned long long merge_genid(unsigned int rrn1, unsigned int rrn2);

const char *breq2a(int req);
void epoch2a(int epoch, char *buf, size_t buflen);
int check_current_schemas(void);
void showdbenv(struct dbenv *dbenv);

void clean_exit(void);

void set_target_lsn(uint32_t logfile, uint32_t logbyte);
void push_next_log(void);

void print_verbose_convert_failure(struct ireq *iq,
                                   const struct convert_failure *fail_reason,
                                   char *fromtag, char *totag);
void convert_failure_reason_str(const struct convert_failure *reason,
                                const char *table, const char *fromtag,
                                const char *totag, char *out, size_t outlen);
int convert_sql_failure_reason_str(const struct convert_failure *reason,
                                   char *out, size_t outlen);

/* These flags are used to customise the behaviour of the high level
 * add/upd/del record functions. */
enum {
    /* don't trigger any stored procedures */
    RECFLAGS_NO_TRIGGERS = 1 << 0,
    /* don't use constraints or defer index operations */
    RECFLAGS_NO_CONSTRAINTS = 1 << 1,
    /* if the schema is not dynamic then bzero the nulls map */
    RECFLAGS_DYNSCHEMA_NULLS_ONLY = 1 << 2,
    /* called from update cascade code, affects key operations */
    RECFLAGS_UPD_CASCADE = 1 << 3,
    /* use .NEW..ONDISK rather than .ONDISK */
    RECFLAGS_NEW_SCHEMA = 1 << 4,
    /* use input genid if in dtastripe mode */
    RECFLAGS_KEEP_GENID = 1 << 5,
    /* don't add blobs and ignore blob buffers - used in planned schema
     * changes since we don't touch blob files. */
    RECFLAGS_NO_BLOBS = 1 << 6,
    /* Used for block/sock/offsql updates to indicate that if a blob is not
     * provided then it should be NULLed out.  In this mode all blobs for
     * the record that are non-NULL will be given. */
    RECFLAGS_DONT_SKIP_BLOBS = 1 << 7,
    RECFLAGS_ADD_FROM_SC_LOGICAL = 1 << 8,
    /* used for upgrade record */
    RECFLAGS_UPGRADE_RECORD = RECFLAGS_DYNSCHEMA_NULLS_ONLY | RECFLAGS_KEEP_GENID | RECFLAGS_NO_TRIGGERS |
                              RECFLAGS_NO_CONSTRAINTS | RECFLAGS_NO_BLOBS | 1 << 9,
    RECFLAGS_IN_CASCADE = 1 << 10,
    RECFLAGS_DONT_LOCK_TBL = 1 << 11,
    RECFLAGS_COMDBG_FROM_LE = 1 << 12,
    RECFLAGS_INLINE_CONSTRAINTS = 1 << 13,

    RECFLAGS_MAX = 1 << 13
};

/* flag codes */
enum fresp_flag_code {
    FRESP_FLAG_NONE = 0,
    FRESP_FLAG_RETRY = 1, /* client should retry sending full query.*/
    FRESP_FLAG_CLOSE =
        2 /* If set, the client should close connection, rather than donate. */
};

/* high level record manipulation */
int add_record(struct ireq *iq, void *trans, const uint8_t *p_buf_tag_name,
               const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
               const uint8_t *p_buf_rec_end, const unsigned char fldnullmap[32],
               blob_buffer_t *blobs, size_t maxblobs, int *opfailcode,
               int *ixfailnum, int *rrn, unsigned long long *genid,
               unsigned long long ins_keys, int opcode, int blkpos, int flags,
               int rec_flags);

int upgrade_record(struct ireq *iq, void *trans, unsigned long long vgenid,
                   uint8_t *p_buf_rec, const uint8_t *p_buf_rec_end,
                   int *opfailcode, int *ixfailnum, int opcode, int blkpos);

int upd_record(struct ireq *iq, void *trans, void *primkey, int rrn,
               unsigned long long vgenid, const uint8_t *p_buf_tag_name,
               const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
               const uint8_t *p_buf_rec_end, uint8_t *p_buf_vrec,
               const uint8_t *p_buf_vrec_end,
               const unsigned char fldnullmap[32], int *updCols,
               blob_buffer_t *blobs, size_t maxblobs, unsigned long long *genid,
               unsigned long long ins_keys, unsigned long long del_keys,
               int *opfailcode, int *ixfailnum, int opcode, int blkpos,
               int flags);

int del_record(struct ireq *iq, void *trans, void *primkey, int rrn,
               unsigned long long genid, unsigned long long del_keys,
               int *opfailcode, int *ixfailnum, int opcode, int flags);

int updbykey_record(struct ireq *iq, void *trans, const uint8_t *p_buf_tag_name,
                    const uint8_t *p_buf_tag_end, uint8_t *p_buf_rec,
                    const uint8_t *p_buf_rec_end, const char *keyname,
                    const unsigned char fldnullmap[32], blob_buffer_t *blobs,
                    size_t maxblobs, int *opfailcode, int *ixfailnum, int *rrn,
                    unsigned long long *genid, int opcode, int blkpos,
                    int flags);

/*
int add_key(struct ireq *iq, void *trans,
        int ixnum, void *key,
        int rrn, unsigned long long genid,
        void *od_dta, size_t od_len,
        const uint8_t *p_buf_req_start, const uint8_t *p_buf_req_end,
        int opcode, int blkpos,
        int *opfailcode,
        int flags);
*/

int del_new_record(struct ireq *iq, void *trans, unsigned long long genid,
                   unsigned long long del_keys, const void *old_dta,
                   blob_buffer_t *idx_blobs, int verify_retry);

int upd_new_record(struct ireq *iq, void *trans, unsigned long long oldgenid,
                   const void *old_dta, unsigned long long newgenid,
                   const void *new_dta, unsigned long long ins_keys,
                   unsigned long long del_keys, int nd_len, const int *updCols,
                   blob_buffer_t *blobs, int deferredAdd,
                   blob_buffer_t *del_idx_blobs, blob_buffer_t *add_idx_blobs,
                   int verify_retry);

int upd_new_record_add2indices(struct ireq *iq, void *trans,
                               unsigned long long newgenid, const void *new_dta,
                               int nd_len, unsigned long long ins_keys,
                               int use_new_tag, blob_buffer_t *blobs,
                               int verify);

void blob_status_to_blob_buffer(blob_status_t *bs, blob_buffer_t *bf);
int save_old_blobs(struct ireq *iq, void *trans, const char *tag, const void *record,
                   int rrn, unsigned long long genid, blob_status_t *blobs);

void start_backend_request(struct dbenv *env);
void start_exclusive_backend_request(struct dbenv *env);
void end_backend_request(struct dbenv *env);

struct query_info *new_query_info(void);
void free_query_info(struct query_info *info);

/* flags to pass down to sqlite glue layer */
enum { NO_REAL_TRANSACTIONS = 1 };

struct conninfo {
    int pindex;
    int node;
    int pid;
    char pename[8];
};
enum { CONNINFO_LEN = 4 + 4 + 4 + (1 * 8) };
BB_COMPILE_TIME_ASSERT(conninfo_size, sizeof(struct conninfo) == CONNINFO_LEN);

enum {
    RESERVED_SZ = sizeof(int) /* Size reserved inside a buffer.
                                 Used to pack things at end of buffer. */
};

struct sql_thread *start_sql_thread(void);
struct sqlclntstate;
int initialize_shadow_trans(struct sqlclntstate *, struct sql_thread *thd);
void get_current_lsn(struct sqlclntstate *clnt);
void done_sql_thread(void);
int sql_debug_logf(struct sqlclntstate *clnt, const char *func, int line,
                   const char *fmt, ...);

enum { LOG_DEL_ABS_ON, LOG_DEL_ABS_OFF, LOG_DEL_REFRESH };
int log_delete_is_stopped(void);
void log_delete_counter_change(struct dbenv *dbenv, int action);
void log_delete_add_state(struct dbenv *dbenv, struct log_delete_state *state);
void log_delete_rem_state(struct dbenv *dbenv, struct log_delete_state *state);

void compr_print_stats();
void print_tableparams();

int is_node_up(const char *host);

/* Prototypes for our berkeley hacks.. */
void berk_write_alarm_ms(int x);
void berk_read_alarm_ms(int x);
void berk_fsync_alarm_ms(int x);
void berk_set_long_trace_func(void (*func)(const char *msg));
void berk_init_rep_lockobj(void);

long long get_unique_longlong(struct dbenv *env);
void block_new_requests(struct dbenv *dbenv);
void allow_new_requests(struct dbenv *dbenv);

int get_next_seqno(void *tran, long long *seqno);
int add_oplog_entry(struct ireq *iq, void *trans, int type, void *logrec,
                    int logsz);
int local_replicant_write_clear(struct ireq *in_iq, void *in_trans,
                                struct dbtable *db);
long long get_record_unique_id(struct dbtable *db, void *rec);
void cancel_sql_statement(int id);
void cancel_sql_statement_with_cnonce(const char *cnonce);

struct client_query_stats *get_query_stats_from_thd();

void process_nodestats(void);
void nodestats_report(FILE *fh, const char *prefix, int disp_rates);
void nodestats_node_report(FILE *fh, const char *prefix, int disp_rates,
                           char *host);
struct rawnodestats *get_raw_node_stats(const char *task, const char *stack,
                                        char *host, int fd, int is_ssl);
int release_node_stats(const char *task, const char *stack, char *host);
struct summary_nodestats *get_nodestats_summary(unsigned *nodes_cnt,
                                                int disp_rates);

int peer_dropped_connection_sbuf(SBUF2 *);
int peer_dropped_connection(struct sqlclntstate *);

void osql_set_replay(const char *file, int line, struct sqlclntstate *clnt,
                     int replay);

int sql_set_transaction_mode(sqlite3 *db, struct sqlclntstate *clnt, int mode);
void perror_errnum(const char *s, int errnum);

void watchdog_disable(void);
void watchdog_enable(void);

void create_old_blkseq_thread(struct dbenv *dbenv);
void debug_traverse_data(char *tbl);
int add_gtid(struct ireq *iq, int source_db, tranid_t id);
int rem_gtid(struct ireq *iq, tranid_t id);

/* Wrappers around berkeley lock code (gut says bad idea, brain insists) */
int locks_get_locker(unsigned int *lid);
int locks_lock_row(unsigned int lid, unsigned long long genid);
int locks_release_locker(unsigned int lid);

void berkdb_use_malloc_for_regions(void);
int get_slotnum_from_buf(struct dbtable *db, char *tagname, void *buf, void *nulls);

int backend_lib_readlock(struct dbenv *dbenv);
int backend_lib_rellock(struct dbenv *dbenv);

void dbuse_enter_reader(void);
void dbuse_enter_writer(void);
void dbuse_leave(void);

int wake_up_coordinated_transaction(struct ireq *iq);
int should_handle_optran_inline(struct ireq *iq);

int find_record_older_than(struct ireq *iq, void *tran, int timestamp,
                           void *rec, int *reclen, int maxlen,
                           unsigned long long *genid);

extern int gbl_exclusive_blockop_qconsume;
extern pthread_rwlock_t gbl_block_qconsume_lock;

extern FILE *twophaselog;

struct coherent_req {
    int coherent;
    int fromdb;
};
enum { COHERENT_REQ_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(coherent_req_len,
                       sizeof(struct coherent_req) == COHERENT_REQ_LEN);

struct make_node_incoherent_req {
    int node;
};
enum { MAKE_NODE_INCOHERENT_REQ_LEN = 4 };
BB_COMPILE_TIME_ASSERT(make_node_incoherent_req,
                       sizeof(struct make_node_incoherent_req) ==
                           MAKE_NODE_INCOHERENT_REQ_LEN);

#define BUFADDR(x) ((void *)&((int *)x)[-2])
#define is_good_ix_find_rc(rc)                                                 \
    ((rc) == IX_FND || (rc) == IX_NOTFND || (rc) == IX_FNDMORE ||              \
     (rc) == IX_PASTEOF)

void mtrap(char *msg);

int get_file_lwm(int lwmp[2]);
int put_file_lwm(int lwm[2]);

void logtwophase(char *fmt, ...);

void abort_participant_threads(void);

/* Transaction states */
enum {
    TRAN_STARTED = 0,
    TRAN_COMMIT = 1,
    TRAN_ABORT = 2,
    TRAN_LOGGED = 3 /* for logging transactions for later replay */
};

void update_remaining_active_gtids(void);
void dump_proxy_config(void);
void proxy_init(void);

/* TODO start move to comdb2_dbinfo.h */

struct db_proxy_config_rsp {
    int prccom[2];
    int nlines;
    int off;
};
enum { DB_PROXY_CONFIG_RSP_NO_HDR_LEN = 4 + 4 };
/* TODO end move to comdb2_dbinfo.h */

struct flddtasizes {
    int type;
    int nflds;
    int fldszs[MAXCOLUMNS];
};
enum {
    FLDDTASIZES_NO_FLDSZS_LEN = 4 + 4,
    FLDDTASIZES_FLDSZ_LEN = 4,
    FLDDTASIZES_STRUCT_LEN =
        FLDDTASIZES_NO_FLDSZS_LEN + (FLDDTASIZES_FLDSZ_LEN * MAXCOLUMNS)
};
BB_COMPILE_TIME_ASSERT(flddtasizes_size,
                       sizeof(struct flddtasizes) == FLDDTASIZES_STRUCT_LEN);

uint8_t *flddtasizes_put(const struct flddtasizes *p_flddtasizes,
                         uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *flddtasizes_get(struct flddtasizes *p_flddtasizes,
                               const uint8_t *p_buf, const uint8_t *p_buf_end);
uint8_t *get_prox2_config_info_put(uint8_t *p_buf, const uint8_t *p_buf_end);
int blkseq_num_in_progress(void);

extern int gbl_coordinator_parallel_transaction_threshold;
extern int gbl_num_rr_rejected;
extern int gbl_sql_time_threshold;
extern double gbl_sql_cost_error_threshold;

extern int gbl_allow_mismatched_tag_size;

uint8_t *req_hdr_put(const struct req_hdr *p_req_hdr, uint8_t *p_buf,
                     const uint8_t *p_buf_end);
const uint8_t *req_hdr_get(struct req_hdr *p_req_hdr, const uint8_t *p_buf,
                           const uint8_t *p_buf_end, int comdbg_flags);

uint8_t *coherent_req_put(const struct coherent_req *p_coherent_req,
                          uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *coherent_req_get(struct coherent_req *p_coherent_req,
                                const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *make_node_incoherent_req_put(
    const struct make_node_incoherent_req *p_make_node_incoherent_req,
    uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *make_node_incoherent_req_get(
    struct make_node_incoherent_req *p_make_node_incoherent_req,
    const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *db_info2_req_no_hdr_put(const struct db_info2_req *p_db_info2_req,
                                 uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *db_info2_req_no_hdr_get(struct db_info2_req *p_db_info2_req,
                                       const uint8_t *p_buf,
                                       const uint8_t *p_buf_end);

/* TODO start move to comdb2_dbinfo.h */
enum {
    DBINFOPC_NO_DATA_LEN = 4 + 4,
    DBINFOPC_NO_DATA_PAD_TAIL = (1 * 3),
    DBINFOPC_STRUCT_LEN =
        DBINFOPC_NO_DATA_LEN + (1 * 1) + DBINFOPC_NO_DATA_PAD_TAIL,
};
BB_COMPILE_TIME_ASSERT(dbinfopc_size,
                       sizeof(struct dbinfopc) == DBINFOPC_STRUCT_LEN);
/* TODO end move to comdb2_dbinfo.h */
uint8_t *dbinfopc_no_data_put(const struct dbinfopc *p_dbinfopc, uint8_t *p_buf,
                              const uint8_t *p_buf_end);
const uint8_t *dbinfopc_no_data_get(struct dbinfopc *p_dbinfopc,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *
db_info2_resp_no_hdr_no_data_put(const struct db_info2_resp *p_db_info2_resp,
                                 uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
db_info2_resp_no_hdr_no_data_get(struct db_info2_resp *p_db_info2_resp,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *db_info2_tbl_info_no_tblinfo_put(
    const struct db_info2_tbl_info *p_db_info2_tbl_info, uint8_t *p_buf,
    const uint8_t *p_buf_end);
const uint8_t *
db_info2_tbl_info_no_tblinfo_get(struct db_info2_tbl_info *p_db_info2_tbl_info,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *tblinfo_no_keyinfo_put(const struct tblinfo *p_tblinfo, uint8_t *p_buf,
                                const uint8_t *p_buf_end);
const uint8_t *tblinfo_no_keyinfo_get(struct tblinfo *p_tblinfo,
                                      const uint8_t *p_buf,
                                      const uint8_t *p_buf_end);

uint8_t *keyinfo_put(const struct keyinfo *p_keyinfo, uint8_t *p_buf,
                     const uint8_t *p_buf_end);
const uint8_t *keyinfo_get(struct keyinfo *p_keyinfo, const uint8_t *p_buf,
                           const uint8_t *p_buf_end);

uint8_t *db_info2_cluster_info_no_node_info_put(
    const struct db_info2_cluster_info *p_db_info2_cluster_info, uint8_t *p_buf,
    const uint8_t *p_buf_end);
const uint8_t *db_info2_cluster_info_no_node_info_get(
    struct db_info2_cluster_info *p_db_info2_cluster_info, const uint8_t *p_buf,
    const uint8_t *p_buf_end);

uint8_t *node_info_put(const struct node_info *p_node_info, uint8_t *p_buf,
                       const uint8_t *p_buf_end);
const uint8_t *node_info_get(struct node_info *p_node_info,
                             const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *db_info2_stats_put(const struct db_info2_stats *p_db_info2_stats,
                            uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *db_info2_stats_get(struct db_info2_stats *p_db_info2_stats,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);

uint8_t *db_proxy_config_rsp_no_hdr_no_lines_put(
    const struct db_proxy_config_rsp *p_db_proxy_config_rsp, uint8_t *p_buf,
    const uint8_t *p_buf_end);
const uint8_t *db_proxy_config_rsp_no_hdr_no_lines_get(
    struct db_proxy_config_rsp *p_db_proxy_config_rsp, const uint8_t *p_buf,
    const uint8_t *p_buf_end);

uint8_t *db_info2_unknown_put(const struct db_info2_unknown *p_db_info2_unknown,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *db_info2_unknown_get(struct db_info2_unknown *p_db_info2_unknown,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

/* Struct tm accessors */
uint8_t *tm_put(const cdb2_tm_t *p_tm, uint8_t *p_buf,
                const uint8_t *p_buf_end);
const uint8_t *tm_get(cdb2_tm_t *p_tm, const uint8_t *p_buf,
                      const uint8_t *p_buf_end);

/* Client datetime accessors */
uint8_t *client_datetime_put(const cdb2_client_datetime_t *p_client_datetime,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *client_datetime_get(cdb2_client_datetime_t *p_client_datetime,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);
uint8_t *
client_extended_datetime_put(const cdb2_client_datetime_t *p_client_datetime,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
client_datetime_get_switch(cdb2_client_datetime_t *p_client_datetime,
                           const uint8_t *p_buf, const uint8_t *p_buf_end,
                           int little_endian);
uint8_t *
client_datetime_put_switch(const cdb2_client_datetime_t *p_client_datetime,
                           uint8_t *p_buf, const uint8_t *p_buf_end,
                           int little_endian);

/* Interval accessors */
uint8_t *client_intv_ym_put(const cdb2_client_intv_ym_t *p_client_intv_ym,
                            uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
client_intv_ym_little_put(const cdb2_client_intv_ym_t *p_client_intv_ym,
                          uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *client_intv_ds_put(const cdb2_client_intv_ds_t *p_client_intv_ds,
                            uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
client_intv_ds_little_put(const cdb2_client_intv_ds_t *p_client_intv_ds,
                          uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *client_intv_ds_get(cdb2_client_intv_ds_t *p_client_intv_ds,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);

const uint8_t *
client_intv_ds_little_get(cdb2_client_intv_ds_t *p_client_intv_ds,
                          const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *client_intv_dsus_put(const cdb2_client_intv_dsus_t *p_client_intv_dsus,
                              uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
client_intv_dsus_little_put(const cdb2_client_intv_dsus_t *p_client_intv_dsus,
                            uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *client_intv_dsus_get(cdb2_client_intv_dsus_t *p_client_intv_dsus,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

const uint8_t *
client_intv_dsus_little_get(cdb2_client_intv_dsus_t *p_client_intv_dsus,
                            const uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *client_intv_ym_get(cdb2_client_intv_ym_t *p_client_intv_ym,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);

const uint8_t *
client_intv_ym_little_get(cdb2_client_intv_ym_t *p_client_intv_ym,
                          const uint8_t *p_buf, const uint8_t *p_buf_end);

struct dbtable *systable_get_db(int systableid);

extern int gbl_num_contexts;
extern int gbl_buffers_per_context;

char *osql_sorese_type_to_str(int stype);
extern int gbl_malloc_regions;

void berkdb_use_malloc_for_regions_with_callbacks(void *mem,
                                                  void *(*alloc)(void *, int),
                                                  void (*free)(void *, void *));

extern int gbl_rowlocks;
extern int gbl_disable_tagged_api;
extern int gbl_disable_tagged_api_writes;
extern int gbl_snapisol;
extern int gbl_new_snapisol;
extern int gbl_new_snapisol_logging;
extern int gbl_new_snapisol_asof;
extern int gbl_update_shadows_interval;
extern int gbl_lowpri_snapisol_sessions;

/* stats */
/* non-sql request service times (last minute, last hour, since start) */
extern struct quantize *q_min;
extern struct quantize *q_hour;
extern struct quantize *q_all;
/* sql query times */
extern struct quantize *q_sql_min;
extern struct quantize *q_sql_hour;
extern struct quantize *q_sql_all;
/* sql #steps */
extern struct quantize *q_sql_steps_min;
extern struct quantize *q_sql_steps_hour;
extern struct quantize *q_sql_steps_all;

extern int gbl_stop_thds_time;
extern int gbl_stop_thds_time_threshold;
extern pthread_mutex_t stop_thds_time_lk;

int trans_commit_logical_tran(void *trans, int *bdberr);
uint64_t bb_berkdb_fasttime(void);
extern int gbl_berk_track_cursors;

void osql_checkboard_foreach_serial_dummy(bdb_osql_trn_t *tran);

int trans_commit_shadow(void *trans, int *bdberr);
int trans_abort_shadow(void **trans, int *bdberr);

void handle_proxy_lrl_line(char *line);
int ftable_init(void);

extern int gbl_disallow_null_blobs;
extern int gbl_inplace_blobs;
extern int gbl_inplace_blob_optimization;
extern int gbl_osql_blob_optimization;
extern int gbl_force_notnull_static_tag_blobs;
extern int gbl_key_updates;

extern unsigned long long gbl_untouched_blob_cnt;
extern unsigned long long gbl_update_genid_blob_cnt;
extern unsigned long long gbl_inplace_blob_cnt;
extern unsigned long long gbl_delupd_blob_cnt;
extern unsigned long long gbl_addupd_blob_cnt;

void bind_verify_indexes_query(sqlite3_stmt *stmt, void *sm);
int verify_indexes_column_value(struct sqlclntstate *clnt, sqlite3_stmt *stmt, void *sm);

void verify_schema_change_constraint(struct ireq *iq, void *trans,
                                     unsigned long long newgenid, void *od_dta,
                                     unsigned long long ins_keys);

enum dirtype { DIR_DB = 1, DIR_TMP = 2, DIR_TRANSACTION = 3 };

extern double gbl_querylimits_maxcost;
extern int gbl_querylimits_tablescans_ok;
extern int gbl_querylimits_temptables_ok;

extern double gbl_querylimits_maxcost_warn;
extern int gbl_querylimits_tablescans_warn;
extern int gbl_querylimits_temptables_warn;

char *get_full_filename(char *path, int pathlen, enum dirtype type, char *name,
                        ...);
int query_limit_cmd(char *line, int llen, int toff);

int is_valid_tablename(char *tbl);

/* defined in toproxy.c */
void reload_proxy_lrl_lines(char *lrlfile);

/* defined in sqlglue.c */
void set_throttle(int num);

/* set default log_file to stderr */
void verify_init(void);
void osql_checkboard_check_down_nodes(char *host);

/**
 * Try to retrieve a genid in the simplest form
 * We assume no data corruption (i.e. we don't try to validate
 * the row, that would be to slow and beyond this code purpose
 * Returns 0 if not found, 1 if found, -1 if error
 *
 */
int ix_check_genid(struct ireq *iq, void *trans, unsigned long long genid,
                   int *bdberr);
int ix_check_genid_wl(struct ireq *iq, void *trans, unsigned long long genid,
                      int *bdberr);
int ix_check_update_genid(struct ireq *iq, void *trans,
                          unsigned long long genid, int *bdberr);

int vtag_to_ondisk(const dbtable *db, uint8_t *rec, int *len, uint8_t ver,
                   unsigned long long genid);
int vtag_to_ondisk_vermap(const dbtable *db, uint8_t *rec, int *len,
                          uint8_t ver);

int get_origin_mach(char *origin);
void comdb2_die(int abort);

/* tcm test case enums */
enum { TCM_PARENT_DEADLOCK = 1, TCM_MAX = 1 };

/* tagged api big or little endian. */
enum { TAGGED_API_BIG_ENDIAN = 1, TAGGED_API_LITTLE_ENDIAN = 2 };

typedef enum {
    SNAP_IMPL_ORIG,
    SNAP_IMPL_NEW,
    SNAP_IMPL_MODSNAP,
} snap_impl_enum;

extern int gbl_check_schema_change_permissions;

extern int gbl_support_datetime_in_triggers;

extern int gbl_use_block_mode_status_code;

extern int gbl_fk_allow_prefix_keys;
extern int gbl_fk_allow_superset_keys;
extern long long gbl_converted_blocksql_requests;
extern int gbl_sql_tranlevel_default;
extern int gbl_sql_tranlevel_preserved;

void berkdb_iopool_process_message(char *line, int lline, int st);
void stop_trickle_threads();

uint8_t *db_info2_iostats_put(const struct db_info2_iostats *p_iostats,
                              uint8_t *p_buf, const uint8_t *p_buf_end);

extern int gbl_log_fstsnd_triggers;

void create_watchdog_thread(struct dbenv *);

int get_max_reclen(struct dbenv *);

void testrep(int niter, int recsz);
int sc_request_disallowed(SBUF2 *sb);
int dump_spfile(const char *file);
int dump_user_version_spfile(const char *file);
int read_spfile(const char *file);
int read_user_version_spfile(const char *file);

struct bdb_cursor_ifn;

int recover_deadlock_simple(bdb_state_type *bdb_state);
int recover_deadlock_flags(bdb_state_type *, struct sqlclntstate *,
                           struct bdb_cursor_ifn *, int sleepms,
                           const char *func, int line, uint32_t flags);
#define recover_deadlock(state, clnt, cur, sleepms) recover_deadlock_flags(state, clnt, cur, sleepms, __func__, __LINE__, RECOVER_DEADLOCK_PTRACE)
int pause_pagelock_cursors(void *arg);
int count_pagelock_cursors(void *arg);
int compare_indexes(const char *table, FILE *out);
void freedb(dbtable *db);

extern int gbl_parallel_recovery_threads;
extern int gbl_core_on_sparse_file;
extern int gbl_check_sparse_files;

extern int __slow_memp_fget_ns;
extern int __slow_read_ns;
extern int __slow_write_ns;

#include "dbglog.h"

void handle_testcompr(SBUF2 *sb, const char *table);
void handle_setcompr(SBUF2 *);
void handle_rowlocks_enable(SBUF2 *);
void handle_rowlocks_enable_master_only(SBUF2 *);
void handle_rowlocks_disable(SBUF2 *);

extern int gbl_decimal_rounding;
extern int gbl_report_sqlite_numeric_conversion_errors;

extern int dfp_conv_check_status(void *pctx, char *from, char *to);

void fix_constraint_pointers(dbtable *db, dbtable *newdb);
void set_odh_options(dbtable *);
void set_odh_options_tran(dbtable *db, tran_type *tran);
void transfer_db_settings(dbtable *olddb, dbtable *newdb);
int reload_after_bulkimport(dbtable *, tran_type *);
int reload_db_tran(dbtable *, tran_type *);
int debug_this_request(int until);

extern int gbl_disable_stable_for_ipu;

extern int gbl_debug_memp_alloc_size;

extern int gbl_max_sql_hint_cache;

/* Remote cursor support */
/* use portmux to open an SBUF2 to local db or proxied db */
SBUF2 *connect_remote_db(const char *protocol, const char *dbname, const char *service, char *host, int use_cache,
                         int force_rte);
int get_rootpage_numbers(int nums);

void sql_dump_hints(void);

extern int gbl_disable_exit_on_thread_error;

extern int gbl_support_sock_lu;

extern int gbl_berkdb_iomap;

extern int gbl_requeue_on_tran_dispatch;
extern long long gbl_verify_tran_replays;

extern int gbl_repscore;

extern int gbl_max_verify_retries;

extern int gbl_surprise;

extern int gbl_check_wrong_db;

extern int gbl_debug_sql_opcodes;

void set_bdb_option_flags(struct dbtable *, int odh, int ipu, int isc, int ver,
                          int compr, int blob_compr, int datacopy_odh);

int init_table_sequences(struct ireq *iq, tran_type *tran, struct dbtable *);

int delete_table_sequences(tran_type *tran, struct dbtable *);

int rename_table_sequences(tran_type *tran, struct dbtable *, const char *newname);

int alter_table_sequences(struct ireq *iq, tran_type *tran, struct dbtable *old, struct dbtable *new);

void set_bdb_queue_option_flags(struct dbtable *, int odh, int compr,
                                int persist);

extern int gbl_debug_temptables;

extern int gbl_check_sql_source;

extern int gbl_flush_check_active_peer;

extern char *gbl_recovery_options;
extern char *gbl_config_root;

int setup_net_listen_all(struct dbenv *dbenv);

extern int gbl_no_env;

extern int gbl_hostname_refresh_time;

extern int gbl_bbenv;

extern int gbl_maxblobretries;

extern int gbl_sqlite_sortermult;

int printlog(bdb_state_type *bdb_state, int startfile, int startoff,
             int endfile, int endoff);

void stat4dump(int more, char *table, int istrace);

int net_allow_node(struct netinfo_struct *netinfo_ptr, const char *host);

extern int gbl_ctrace_dbdir;
extern int gbl_private_blkseq;
extern int gbl_use_blkseq;

extern int gbl_sc_inco_chk;
extern int gbl_track_queue_time;
extern int gbl_broadcast_check_rmtpol;
extern int gbl_deadlock_policy_override;
extern int gbl_accept_on_child_nets;
extern int gbl_disable_etc_services_lookup;
extern int gbl_sql_random_release_interval;
extern int gbl_debug_queuedb;
extern int gbl_lua_prepare_max_retries;
extern int gbl_lua_prepare_retry_sleep;

/**
 * Schema change that touches a table in any way updates its version
 * Called every time a schema change is successfully done
 */
int table_version_upsert(struct dbtable *db, void *trans, int *bdberr);

/**
 * Retrieves table version or 0 if no entry
 *
 */
unsigned long long table_version_select(struct dbtable *db, tran_type *tran);

/**
 *  Set the version to a specific version, required by timepartition
 *
 */
int table_version_set(tran_type *tran, const char *tablename,
                      unsigned long long version);
/**
 * Interface between partition roller and schema change */
int sc_timepart_add_table(const char *existingTableName,
                          const char *newTableName, struct errstat *err);
int sc_timepart_drop_table(const char *tableName, struct errstat *err);
int sc_timepart_truncate_table(const char *tableName, struct errstat *err,
                               void *partition);

/* SCHEMACHANGE DECLARATIONS*/

// These were all over the place in schemachange, moved here hoping in some
// future refactoring

int compare_tag(const char *table, const char *tag, FILE *out);
int compare_tag_int(struct schema *old, struct schema *new, FILE *out,
                    int strict);
int cmp_index_int(struct schema *oldix, struct schema *newix, char *descr,
                  size_t descrlen);
int get_dbtable_idx_by_name(const char *tablename);
int open_temp_db_resume(struct ireq *iq, struct dbtable *db, char *prefix, int resume,
                        int temp, tran_type *tran);
int find_constraint(struct dbtable *db, constraint_t *ct);

/* END OF SCHEMACHANGE DECLARATIONS*/

/**
 * Disconnect a socket and tries to save it in sockpool
 */
void disconnect_remote_db(const char *protocol, const char *dbname, const char *service, char *host,
                          SBUF2 **psb);

void sbuf2gettimeout(SBUF2 *sb, int *read, int *write);
int sbuf2fread_timeout(char *ptr, int size, int nitems, SBUF2 *sb, int *was_timeout);
unsigned long long verify_indexes(struct dbtable *db, uint8_t *rec,
                                  blob_buffer_t *blobs, size_t maxblobs,
                                  int is_alter);
int verify_check_constraints(struct dbtable *table, uint8_t *rec,
                             blob_buffer_t *blobs, size_t maxblobs,
                             int is_alter);

/* Blob mem. */
extern comdb2bma blobmem; // blobmem for db layer
extern size_t gbl_blobmem_cap;
extern unsigned gbl_blob_sz_thresh_bytes;
extern int gbl_large_str_idx_find;
extern int gbl_mifid2_datetime_range;

/* Query fingerprinting */
extern int gbl_fingerprint_queries;
extern int gbl_prioritize_queries;
extern int gbl_debug_force_thdpool_priority;
extern int gbl_verbose_prioritize_queries;

/* Global switch for perfect checkpoint. */
extern int gbl_use_perfect_ckp;
/* (For testing only) Configure how long a checkpoint
   sleeps before asking MPool to flush. */
extern int gbl_ckp_sleep_before_sync;

int set_rowlocks(void *trans, int enable);

void init_sqlclntstate(struct sqlclntstate *clnt, char *tid);

/* 0: Return null constraint error for not-null constraint violation on updates
   1: Return conversion error instead */
extern int gbl_upd_null_cstr_return_conv_err;

/* Update the tunable at runtime. */
comdb2_tunable_err handle_runtime_tunable(const char *name, const char *value);
/* Update the tunable read from lrl file. */
comdb2_tunable_err handle_lrl_tunable(char *name, int name_len, char *value,
                                      int value_len, int flags);

int db_is_stopped(void);
int db_is_exiting();

int is_tablename_queue(const char *);

int rename_table_options(void *tran, struct dbtable *db, const char *newname);

int comdb2_get_verify_remote_schemas(void);
void comdb2_set_verify_remote_schemas(void);

const char *thrman_get_where(struct thr_handle *thr);

int repopulate_lrl(const char *p_lrl_fname_out);
void plugin_post_dbenv_hook(struct dbenv *dbenv);

extern int64_t gbl_temptable_created;
extern int64_t gbl_temptable_create_reqs;
extern int64_t gbl_temptable_spills;

extern int gbl_disable_tpsc_tblvers;

extern int gbl_osql_odh_blob;
extern int gbl_pbkdf2_iterations;
extern int gbl_bpfunc_auth_gen;
extern int gbl_sqlite_makerecord_for_comdb2;

void dump_client_sql_data(struct reqlogger *logger, int do_snapshot);

int backout_schema_changes(struct ireq *iq, tran_type *tran);
int bplog_schemachange(struct ireq *iq);

extern int gbl_abort_invalid_query_info_key;
extern int gbl_is_physical_replicant;

extern void global_sql_timings_print(void);

/* User password cache */
void init_password_cache();
void destroy_password_cache();

extern int gbl_rcache;
extern int gbl_throttle_txn_chunks_msec;
extern int gbl_sql_release_locks_on_slow_reader;
extern int gbl_fail_client_write_lock;
extern int gbl_server_admin_mode;

void csc2_free_all(void);

int fdb_default_ver_set(int val);

/* hack to temporary allow bools on production stage */
void csc2_allow_bools(void);
void csc2_disallow_bools(void);
int csc2_used_bools(void);

/* Skip spaces and tabs, requires at least one space */
static inline char *skipws(char *str)
{
    if (str) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

#endif /* !INCLUDED_COMDB2_H */
