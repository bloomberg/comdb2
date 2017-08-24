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

#ifndef INCLUDED_COMDB2_H
#define INCLUDED_COMDB2_H

/* DB_RELEASE_NAME moved to gbl_db_release_name
 * Please update gbl_db_release_name when branchings a new release
 */

/* MOVE THESE TO COMDB2_API.H */

#define SQLHERR_LIMIT (-107)
#define SQLHERR_ROLLBACKTOOLARGE (-108)
#define SQLHERR_ROLLBACK_TOOOLD (-109)
#define SQLHERR_ROLLBACK_NOLOG (-110)
#define MAXVER 255

#define SP_FILE_NAME "stored_procedures.sp"
#define REPOP_QDB_FMT "%s/%s.%d.queuedb" /* /dir/dbname.num.ext */

#define DEFAULT_USER "default"
#define DEFAULT_PASSWORD ""

enum { IOTIMEOUTMS = 10000 };

enum {
    SQLF_QUEUE_ME = 1,
    SQLF_FAILDISPATCH_ON = 2,
    SQLF_CONVERTED_BLOSQL = 4,
    SQLREQ_FLAG_WANT_SP_TRACE = 8,
    SQLREQ_FLAG_WANT_SP_DEBUG = 16,
    SQLF_WANT_NEW_ROW_DATA = 32,
    SQLF_WANT_QUERY_EFFECTS = 64,
    SQLF_WANT_READONLY_ACCESS = 128,
    SQLF_WANT_VERIFYRETRY_OFF = 256
};

#define N_BBIPC 2

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
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <pthread.h>
#include <bb_stdint.h>
#include <sbuf2.h>
#include <netinet/in.h>

#include <flibc.h>
#include <endian_core.h>
#include <portmuxapi.h>
#include <epochlib.h>
#include <fsnapf.h>
#include <plhash.h>

#include <list.h>
#include <queue.h>
#include <compile_time_assert.h>
#include <history.h>
#include <comdb2_info.h>
#include <comdb2_dbinfo.h>
#include <comdb2_trn_intrl.h>
#include <sqlthdpool.h>
#include <prefault.h>
#include <quantize.h>
#include <dlmalloc.h>
#include <stdbool.h>

#include "tag.h"
#include "errstat.h"
#include "comdb2_rcodes.h"
#include "sqlquery.pb-c.h"
#include "repl_wait.h"
#include "types.h"
#include "thread_util.h"
#include "request_stats.h"
#include "bdb_osqltrn.h"
#include "thdpool.h"
#include "thrman.h"
#include "comdb2uuid.h"
#include "comdb2_legacy.h"
#include "machclass.h"
#include "tunables.h"

#ifndef LUASP
#include <mem_uncategorized.h>
#include <mem_override.h>
#endif

#include <trigger.h>
#include <cdb2_constants.h>
#include <schema_lk.h>

/* buffer offset, given base ptr & right ptr */
#define BUFOFF(base, right) ((int)(((char *)right) - ((char *)base)))

/* we will delete at most this many per run of purge_old_blkseq */
#define MAXBLKSEQ_PURGE (5 * 1024)

#define MAX_NUM_TABLES 1024
#define MAX_NUM_QUEUES 1024
#define MAX_NUM_SEQUENCES 1024

#define BBIPC_KLUDGE_LEN 8

#define DEC_ROUND_NONE (-1)

enum AUXDB_TYPES {
    AUXDB_NONE = 0
    /* 1 used to be AUXDB_BLKSEQ, but it has been purged */
    ,
    AUXDB_META = 2,
    AUXDB_FSTBLK = 3
};

enum NET_NAMES { NET_REPLICATION, NET_SQL, NET_SIGNAL };
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
    REP_SYNC_FULL = 0 /* run asynchronously */
    ,
    REP_SYNC_SOURCE = 1 /* source node only is synchronized before ack */
    ,
    REP_SYNC_NONE = 2 /* all nodes are sync'd before ack */
    ,
    REP_SYNC_ROOM = 3 /* sync to nodes in my machine room */
    ,
    REP_SYNC_N = 4 /* wait for N machines */
};

enum STMT_CACHE_FLAGS {
    STMT_CACHE_NONE = 0,
    STMT_CACHE_PARAM = 1,
    STMT_CACHE_ALL = 2 /* cache all the queries */
};

enum TRAN_FLAGS {
    TRAN_NO_SYNC =
        1 /* don't wait for acks for this transaction (be careful...) */
    ,
    TRAN_VERIFY = 2 /* before commit verify all the keys */
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
    OP_UPGRADE = 132 /* dummy code for online upgrade */

    ,
    MAXTYPCNT = 132

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
    BLOCK2_SQL = 811,
    BLOCK2_DELOLDER = 812,
    BLOCK2_TRAN = 813,
    BLOCK2_MODNUM = 814,
    BLOCK2_SOCK_SQL = 815,
    BLOCK2_SCSMSK = 816,
    BLOCK2_RECOM = 817,
    BLOCK2_UPDBYKEY = 818,
    BLOCK2_SERIAL = 819,
    BLOCK2_SQL_PARAMS = 820,
    BLOCK2_DBGLOG_COOKIE = 821,
    BLOCK2_PRAGMA = 822,
    BLOCK2_SNAPISOL = 823,
    BLOCK2_SEQV2 = 824,
    BLOCK2_UPTBL = 825,
    BLOCK_MAXOPCODE

    /* Used for some condensed blockop stats; this should be the number of
     * opcodes
     * that there actually really are. */
    ,
    NUM_BLOCKOP_OPCODES = 45
};

/*
   type will identify if there is a new record and type the new record is *
*/
enum OSQL_RPL_TYPE {
    OSQL_RPLINV = 0,
    OSQL_DONE = 1,
    OSQL_USEDB = 2,
    OSQL_DELREC = 3,
    OSQL_INSREC = 4,
    OSQL_CLRTBL = 5,
    OSQL_QBLOB = 6,
    OSQL_UPDREC = 7,
    OSQL_XERR = 8,
    OSQL_UPDCOLS = 9,
    OSQL_DONE_STATS = 10, /* just like OSQL_DONE, but have additional stats */
    OSQL_DBGLOG = 11,
    OSQL_RECGENID = 12,
    OSQL_UPDSTAT = 13,
    OSQL_EXISTS = 14,
    OSQL_SERIAL = 15,
    OSQL_SELECTV = 16,
    OSQL_DONE_SNAP = 17,
    OSQL_SCHEMACHANGE = 18,
    OSQL_BPFUNC = 19,
    OSQL_DBQ_CONSUME = 20,
    OSQL_DELETE = 21, /* new osql type to support partial indexes */
    OSQL_INSERT = 22, /* new osql type to support partial indexes */
    OSQL_UPDATE = 23, /* new osql type to support partial indexes */
    OSQL_DELIDX = 24, /* new osql type to support indexes on expressions */
    OSQL_INSIDX = 25, /* new osql type to support indexes on expressions */
    OSQL_DBQ_CONSUME_UUID = 26,
    MAX_OSQL_TYPES = 27
};

enum DEBUGREQ { DEBUG_METADB_PUT = 1 };

enum RCODES {
      RC_OK = 0 /*SUCCESS*/
    , ERR_VERIFY = 4 /*failed verify on updwver*/
    , ERR_CORRUPT = 8 /*CORRUPT INDEX*/
    , ERR_BADRRN = 9 /* findbyrrn on bad rrn */
    , ERR_ACCESS = 10 /* access denied */
    , ERR_DTA_FAILED = 11 /*failed operation on data */
    , ERR_INTERNAL = 177 /*internal logic error.*/
    , ERR_REJECTED = 188 /*request rejected*/
    , RC_INTERNAL_FORWARD = 193 /*forwarded block request*/
    , ERR_FAILED_FORWARD = 194 /*block update failed to send to remote*/
    , ERR_READONLY = 195 /*database is currently read-only*/
    , ERR_NOMASTER = 1000 /*database has no master, it is readonly*/
    , ERR_NESTED = 1001 /*this is not master, returns actual master*/
    , ERR_RMTDB_NESTED = 198 /*reserved for use by rmtdb/prox2*/
    , ERR_BADREQ = 199 /*bad request parameters*/
    , ERR_TRAN_TOO_BIG = 208 
    , ERR_BLOCK_FAILED = 220 /*block update failed*/
    , ERR_NOTSERIAL = 230 /*transaction not serializable*/
    , ERR_SC = 240 /*schemachange failed*/
    , RC_INTERNAL_RETRY = 300 /*need to retry comdb upper level request*/
    , ERR_CONVERT_DTA = 301
    , ERR_CONVERT_IX = 301
    , ERR_KEYFORM_UNIMP = 303
    , RC_TRAN_TOO_COMPLEX = 304 /* too many rrns allocated per trans */
    , RC_TRAN_CLIENT_RETRY = 305
    , ERR_BLOB_TOO_LARGE = 306 /* a blob exceeded MAXBLOBLENGTH */
    , ERR_BUF_TOO_SMALL = 307 /* buffer provided too small to fit data */
    , ERR_NO_BUFFER = 308 /* can't get fstsnd buffer */
    , ERR_JAVASP_ABORT = 309 /* stored procedure ordered this trans aborted */
    , ERR_NO_SUCH_TABLE = 310 /* operation tried to use non-existant table */
    , ERR_CALLBACK = 311 /* operation failed due to errors in callback */
    , ERR_TRAN_FAILED = 312 /* could not start of finish transaction */
    , ERR_CONSTR = 313 /* could not complete the operation becouse of
                        constraints in the table*/
    , ERR_SC_COMMIT = 314 /* schema change in its final stages; proxy
                           should retry */
    , ERR_INDEX_DISABLED = 315 /* can't read from a disabled index */
    , ERR_CONFIG_FAILED = 316
    , ERR_NO_RECORDS_FOUND = 317
    , ERR_NULL_CONSTRAINT = 318
    , ERR_VERIFY_PI = 319
    , ERR_UNCOMMITABLE_TXN = 404 /* txn is uncommitable, returns ERR_VERIFY rather than retry */
    , ERR_INCOHERENT = 996 /* prox2 understands it should retry another node for 996 */
    , ERR_NO_AUXDB = 2000 /* requested auxiliary database not available */
    , ERR_SQL_PREP = 2001 /* block sql error in sqlite3_prepare */
    , ERR_LIMIT = 2002 /* sql request exceeds max cost */
    , ERR_NOT_DURABLE = 2003 /* commit didn't make it to a majority */
};

enum dbt_api_return_codes {
    DB_RC_SUCCESS = 0,

    /* FIND RCODES */
    DB_RC_FND = 0,
    DB_RC_MORE = 1,
    DB_ERR_NOTFND = 2,
    DB_ERR_NOTFNDEOF = 3,
    DB_ERR_CONVFIND = 4,
    DB_ERR_CONVMORE = 5,
    DB_ERR_CONVNOTFND = 6,
    DB_ERR_CONVNOTFNDEOF = 7,
    DB_ERR_EMPTY_TABLE = 8,
    DB_ERR_NOTFNDRRN = 9,

    /* GENERAL RCODES */
    DB_ERR_ACCESS = 100,       /* 199 */
    DB_ERR_BAD_REQUEST = 110,  /* 199 */
    DB_ERR_BAD_COMM_BUF = 111, /* 998 */
    DB_ERR_BAD_COMM = 112,     /* 999 */
    DB_ERR_CONV_FAIL = 113,    /* 301 */
    DB_ERR_NONKLESS = 114,     /* 212 */
    DB_ERR_MALLOC = 115,
    DB_ERR_NOTSUPPORTED = 116,
    DB_ERR_BADCTRL = 117,
    DB_ERR_DYNTAG_LOAD_FAIL = 118,
    /* GENERAL BLOCK TRN RCODES */
    DB_RC_TRN_OK = 0,
    DB_ERR_TRN_DUP = 1,             /* dup add 2 , returned with 220 before */
    DB_ERR_TRN_VERIFY = 2,          /* verify 4, returned with 220 before */
    DB_ERR_TRN_FKEY = 3,
    DB_ERR_TRN_NULL_CONSTRAINT = 4, /* 318 */
    DB_ERR_TRN_BUF_INVALID = 200,   /* 105 */
    DB_ERR_TRN_BUF_OVERFLOW = 201,  /* 106 */
    DB_ERR_TRN_OPR_OVERFLOW = 202,  /* 205 */
    DB_ERR_TRN_FAIL = 203,          /* 220 */
    DB_ERR_TRN_DB_FAIL = 204,       /* 220 */
    DB_ERR_TRN_DB_CONN = 205,
    DB_ERR_TRN_DB_IO = 206,
    DB_ERR_TRN_NOT_SERIAL = 230,

    /* INTERNAL DB ERRORS */
    DB_ERR_INTR_NO_MASTER = 300,
    DB_ERR_INTR_FWD = 301,
    DB_ERR_INTR_NESTED_FWD = 302,
    DB_ERR_INTR_MASTER_REJECT = 303,
    DB_ERR_INTR_GENERIC = 304,
    DB_ERR_INTR_READ_ONLY = 305,
    DB_ERR_INTR_BLOB_RETR = 306,

    /* TZNAME SET FAILURE */
    DB_ERR_TZNAME_FAIL = 401,

    DB_RC_RNG_OK = 0,
    DB_RC_RNG_DONE = 1
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

    META_BLOBSTRIPE_GENID_RRN =
        -3, /* in this rrn we store the genid of the table
               when it was converted to blobstripe */

    META_STUFF_RRN = -4, /* used by pushlogs.c to do "stuff" to the database
                           until we get past a given lsn. */
    META_ONDISK_HEADER_RRN = -5, /* do we have the new ondisk header? */
    META_COMPRESS_RRN =
        -6, /* which compression algorithm to use for new records (if any) */
    META_COMPRESS_BLOBS_RRN = -7, /* and which to use for blobs. */
    META_FILEVERS = -8,           /* 64 bit id for filenames */
    META_FILE_LWM = -9,           /* int - lower deleteable log file */
    META_INSTANT_SCHEMA_CHANGE = -10,
    META_DATACOPY_ODH = -11,
    META_INPLACE_UPDATES = -12,
    META_BTHASH = -13
};

enum CONSTRAINT_FLAGS {
    CT_UPD_CASCADE = 0x00000001,
    CT_DEL_CASCADE = 0x00000002,
    CT_BLD_SKIP = 0x00000004
};

enum {
    DBTYPE_UNTAGGED_TABLE = 0,
    DBTYPE_TAGGED_TABLE = 1,
    DBTYPE_QUEUE = 2,
    DBTYPE_MORESTRIPE = 3, /* fake type used in schema change code */
    DBTYPE_QUEUEDB = 4,
    DBTYPE_SEQUENCE = 5
};

/* Copied verbatim from bdb_api.h since we don't expose that file to most
 * modules. */
enum {
    COMDB2_THR_EVENT_DONE_RDONLY = 0,
    COMDB2_THR_EVENT_START_RDONLY = 1,
    COMDB2_THR_EVENT_DONE_RDWR = 2,
    COMDB2_THR_EVENT_START_RDWR = 3
};

/* index disable flags */
enum { INDEX_READ_DISABLED = 1, INDEX_WRITE_DISABLED = 2 };

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

enum deferred_option_level {
   DEFERRED_SEND_COMMAND,
   DEFERRED_LEGACY_DEFAULTS,

   DEFERRED_OPTION_MAX
};

/* Raw stats, kept on a per origin machine basis.  This whole struct is
 * essentially an array of unsigneds.  Please don't add any other data
 * type as this allows us to easily sum it and diff it in a loop in reqlog.c.
 * All of these stats are counters. */
struct rawnodestats {
    unsigned opcode_counts[MAXTYPCNT];
    unsigned blockop_counts[NUM_BLOCKOP_OPCODES];
    unsigned sql_queries;
    unsigned sql_steps;
    unsigned sql_rows;
};
#define NUM_RAW_NODESTATS (sizeof(struct rawnodestats) / sizeof(unsigned))

/* records in sql master db look like this (no appended rrn info) */
struct sqlmdbrectype {
    char type[8];
    char name[20];
    char tblname[20];
    int rootpage;
    char sql[876];
};

#define MAXREF 64

typedef struct {
    short dbnum;
    short ixnum;
} fk_ref_type;

typedef struct {
    short num;
    fk_ref_type ref[MAXREF];
} fk_ref_array_type;

typedef struct {
    struct dbtable *lcltable;
    char *lclkeyname;
    int nrules;
    int flags;
    char *table[MAXCONSTRAINTS];
    char *keynm[MAXCONSTRAINTS];
} constraint_t;

/* SEQUENCE object attributes */
typedef struct {
    int version;            /* Sequence attr struct version */
    char name[MAXTABLELEN]; /* Identifier */

    /* Dispensing */
    sequence_range_t *range_head; /* Pointer to the head range node*/

    /* Basic Attributes */
    long long
        min_val; /* Values dispensed must be greater than or equal to min_val */
    long long
        max_val; /* Values dispensed must be less than or equal to max_val */
    long long start_val; /* Start value for the sequence*/
    long long increment; /* Normal difference between two consecutively
                            dispensed values */
    bool cycle;          /* If cycling values is permitted */

    /* Synchronization with llmeta */
    long long chunk_size;     /* Number of values to allocate from llmeta */
    long long next_start_val; /* Starting value of the next chunk */

    /* Flags */
    char flags; /* Flags for the sequence object*/

    pthread_mutex_t seq_lk; /* mutex for protecting the value dispensing */
} sequence_t;

struct managed_component {
    int dbnum;
    LINKC_T(struct managed_component) lnk;
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
/*
 * We now have different types of db (I overloaded this structure rather than
 * create a new structure because the ireq usedb concept is endemic anyway).
 */
struct dbtable {
    struct dbenv *dbenv; /*chain back to my environment*/
    char *lrlfname;
    char *dbname;

    int dbnum;
    int lrl; /*dat len in bytes*/
    /*index*/
    unsigned short nix; /*number of indices*/

    unsigned short ix_keylen[MAXINDEX]; /*key len in bytes*/
    signed char ix_dupes[MAXINDEX];
    signed char ix_recnums[MAXINDEX];
    signed char ix_datacopy[MAXINDEX];
    signed char ix_collattr[MAXINDEX];
    signed char ix_nullsallowed[MAXINDEX];
    signed char ix_disabled[MAXINDEX];
    struct ireq *iq; /* iq used at sc time */
    int ix_partial;  /* set to 1 if we have partial indexes */
    int ix_expr;     /* set to 1 if we have indexes on expressions */
    int ix_blob;     /* set to 1 if blobs are involved in indexes */

    int is_readonly;
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
    unsigned typcnt[MAXTYPCNT + 1];
    unsigned blocktypcnt[BLOCK_MAXOPCODE];
    unsigned blockosqltypcnt[MAX_OSQL_TYPES];
    unsigned nsql;
    /*prev counters for diff*/
    unsigned prev_typcnt[MAXTYPCNT + 1];
    unsigned prev_blocktypcnt[BLOCK_MAXOPCODE];
    unsigned prev_blockosqltypcnt[MAX_OSQL_TYPES];
    unsigned prev_nsql;
    /* counters for autoanalyze */
    unsigned write_count[RECORD_WRITE_MAX];
    unsigned saved_write_count[RECORD_WRITE_MAX];
    unsigned aa_saved_counter; // zeroed out at autoanalyze
    time_t aa_lastepoch;
    unsigned aa_counter_upd;   // counter which includes updates
    unsigned aa_counter_noupd; // does not include updates

    /* This tables constraints */
    constraint_t constraints[MAXCONSTRAINTS];
    int n_constraints;

    /* Pointers to other table constraints that are directed at this table. */
    constraint_t *rev_constraints[MAXCONSTRAINTS];
    int n_rev_constraints;

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
    unsigned long long *sc_genids; /* schemachange stripe pointers */

    unsigned int sqlcur_ix;  /* count how many cursors where open in ix mode */
    unsigned int sqlcur_cur; /* count how many cursors where open in cur mode */

    char *csc2_schema;
    int csc2_schema_len;

    struct dbstore dbstore[MAXCOLUMNS];
    int odh;
    int version;
    int instant_schema_change;
    int inplace_updates;
    unsigned long long tableversion;

    int do_local_replication;
    int shmflags;

    /* map of tag fields for version to curr schema */
    unsigned int * versmap[MAXVER + 1];
    /* is tag version compatible with ondisk schema */
    uint8_t vers_compat_ondisk[MAXVER + 1];

    /* lock for consumer list */
    pthread_rwlock_t consumer_lk;
};

struct log_delete_state {
    int filenum;
    LINKC_T(struct log_delete_state) linkv;
};

struct coordinated_component {
    LINKC_T(struct coordinated_component) lnk;
    int dbnum;
};

struct deferred_option {
    char *option;
    int line;
    int len;
    LINKC_T(struct deferred_option) lnk;
};

struct lrlfile {
    char *file;
    LINKC_T(struct lrlfile) lnk;
};

struct dbenv {
    char *basedir;
    char *envname;
    int dbnum;

    /*backend db engine handle*/
    void *bdb_attr;     /*engine attributes*/
    void *bdb_callback; /*engine callbacks */

    char *master; /*current master node, from callback*/

    int cacheszkb;
    int cacheszkbmin;
    int cacheszkbmax;
    int override_cacheszkb;

    /*sibling info*/
    int nsiblings;
    char *sibling_hostname[MAXSIBLINGS];
    int sibling_node[MAXSIBLINGS];  /* currently not used */
    int sibling_flags[MAXSIBLINGS]; /* currently not used */
    int sibling_port[MAXSIBLINGS][MAXNETS];
    int listen_fds[MAXNETS];
    /* banckend db engine handle for replication */
    void *handle_sibling;
    void *handle_sibling_offload;
    void *handle_sibling_signal;

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
    /* this is a lonked list of log_delete_stat structs.  If the list is
     * empty then log fil deletion can proceed as normal.  Otherwise we
     * have one or more clients that have requested log deletion to be
     * held up, at least beyond a certain log number.  The log_delete_state
     * structs themselves reside on the stacks of appsock threads. */
    LISTC_T(struct log_delete_state) log_delete_state_list;

    /*counters*/
    int typcnt[MAXTYPCNT + 1];

    /* bdb_environment */
    void *bdb_env;

    /* tables and queues */
    int num_dbs;
    struct dbtable **dbs;
    hash_t *db_hash;
    int num_qdbs;
    struct dbtable **qdbs;
    hash_t *qdb_hash;

    /* sequences */
    int num_sequences;
    sequence_t **sequences;

    /* Special SPs */
    int num_lua_sfuncs;
    char **lua_sfuncs;
    int num_lua_afuncs;
    char **lua_afuncs;

    /* is sql mode enabled? */
    int sql;

    /* enable client side retrys for N seconds */
    int retry;

    int rep_always_wait;

    int pagesize_dta;
    int pagesize_freerec;
    int pagesize_ix;

    int enable_direct_writes;

    pthread_t purge_old_blkseq_tid;
    pthread_t purge_old_files_tid;

    /* stupid - is the purge_old_blkseq thread running? */
    int purge_old_blkseq_is_running;
    int purge_old_files_is_running;
    int exiting; /* are we exiting? */
    int stopped; /* if set, drop requests */
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

    LISTC_T(struct managed_component) managed_participants;
    LISTC_T(struct managed_component) managed_coordinators;
    pthread_mutex_t incoherent_lk;
    int num_incoherent;
    int fallen_offline;
    int manager_dbnum;

    pthread_t watchdog_tid;
    pthread_t watchdog_watcher_tid;

    unsigned txns_committed;
    unsigned txns_aborted;
    unsigned prev_txns_committed;
    unsigned prev_txns_aborted;
    int wait_for_N_nodes;

    LISTC_T(struct deferred_option) deferred_options[DEFERRED_OPTION_MAX];
    LISTC_T(struct lrlfile) lrl_files;
    int shmflags;

    int incoh_notcoherent;
    uint32_t incoh_file, incoh_offset;
    timepart_views_t *timepart_views;

    /* locking for the queue system */
    pthread_mutex_t dbqueue_admin_lk;
    int dbqueue_admin_running;
};

extern struct dbenv *thedb;
extern comdb2_tunables *gbl_tunables;

extern pthread_key_t unique_tag_key;
extern pthread_key_t sqlite3VDBEkey;
extern pthread_key_t query_info_key;

struct req_hdr {
    int ver1;
    short ver2;
    unsigned char luxref;
    unsigned char opcode;
};

enum { REQ_HDR_LEN = 4 + 2 + 1 + 1 };

BB_COMPILE_TIME_ASSERT(req_hdr_len, sizeof(struct req_hdr) == REQ_HDR_LEN);

struct thread_info {
    long long uniquetag;
    long long ct_id_key;
    void *ct_add_table;
    void *ct_del_table;
    void *ct_add_index;
};

/* Unique id for a record.  Note that an RRN is sufficiently unique
   here as long as we only look at this record pre-commit (no other
   transaction can delete the rrn). */
struct modified_record {
    int rrn;
    unsigned long long genid;
    LINKC_T(struct modified_record) lnk;
};

/* key for fstblk records - a 12 byte sequence number generated by the
 * proxy and unique for each transaction. */
typedef struct {
    int int3[3];
} fstblkseq_t;

/* this stores info about the socksql/recom requester */
typedef struct sorese_info {
    unsigned long long rqid; /* not null means active */
    uuid_t uuid;
    char *host; /* sql machine, 0 is local */
    SBUF2 *osqllog; /* used to track sorese requests */
    int type;   /* type, socksql or recom */
    int nops;   /* if no error, how many updated rows were performed */
    int rcout;  /* store here the block proc main error */

    int verify_retries; /* how many times we verify retried this one */
    bool use_blkseq;    /* used to force a blkseq, for locally retried txn */
    bool osql_retry;    /* if this is osql transaction, once sql part
                          finished successful, we set this to one
                          to avoid repeating it if the transaction is reexecuted
                       */

} sorese_info_t;

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
    int keylen;
    char key[MAX_SNAP_KEY_LEN];
} snap_uid_t;

enum { SNAP_UID_LENGTH = 8 + 4 + (4 * 5) + 4 + 64 };

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
/*******************************************************************/
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
    unsigned long long rqid;
    int usedbtablevers;
    int frompid;
    int debug;
    int opcode;
    int luxref;
    uint8_t osql_rowlocks_enable;
    uint8_t osql_genid48_enable;

    /************/
    /* REGION 2 */
    /************/
    uint8_t region2; /* used for offsetof */
    char corigin[80];
    char debug_buf[256];
    char tzname[DB_MAX_TZNAMEDB];
    char sqlhistory[1024];
    snap_uid_t snap_info;

    /************/
    /* REGION 3 */
    /************/
    uint8_t region3; /* used for offsetof */

    uint64_t startus; /*thread handling*/
    /* for waking up socket thread. */
    void *request_data;
    char *tag;
    void *use_handle; /* for fake ireqs, so I can start a transaction */

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

    /* for blocksql purposely we want to have the list of sql
       queries submitted;
       the field is not null if we have any BLOCK2_SQL requests
       THIS IS USED IN BLOCKSQL, WOULD BE NICE TO UNIFY -later-
     */
    void *blocksql_tran;

    /* socksql/recom storage */
    sorese_info_t sorese;

    /* debug osql */
    osql_bp_timings_t timings;

    /* Support for //DBSTATS. */
    SBUF2 *dbglog_file;
    int *nwrites;
    char *sqlhistory_ptr;
    /* List of genids that we've written to detect uncommitable txn's */
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

    struct schema_change_type *sc_pending;
    double cost;
    uint64_t sc_seed;
    uint64_t txnsize;
    unsigned long long last_genid;

    /* if we replicated then these get updated */
    int reptimems;
    int timeoutms;
    int transflags; /* per-transaction flags */

    /* more stats - number of retries done under this request */
    int retries;

    /* count the number of updates and deletes done by this transaction in
     * a live schema change behind the cursor.  This helps us know how many
     * records we've really done (since every update behind the cursor
     * effectively means we have to go back and do that record again). */
    unsigned sc_adds;
    unsigned sc_deletes;
    unsigned sc_updates;

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
    int priority;
    int sqlhistory_len;

    /* Client endian flags. */
    uint8_t client_endian;

    bool have_client_endian : 1;
    bool is_fake : 1;
    bool is_dumpresponse : 1;
    bool is_fromsocket : 1;
    bool is_socketrequest : 1;
    bool is_block2positionmode : 1;

    bool errstrused : 1;
    bool vfy_genid_track : 1;
    bool is_sorese : 1;
    bool have_blkseq : 1;

    bool sc_locked : 1;
    bool have_snap_info : 1;
    bool tranddl : 1;
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
                         const uint8_t *p_buf_end);

/* endianized array_setter for rngext */
uint8_t *array_put(const struct array *p_array, uint8_t *p_buf,
                   const uint8_t *p_buf_end);

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
                               const uint8_t *p_buf, const uint8_t *p_buf_end);

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
    SCAN_PARALLEL = 4 /* creates one thread for each stripe */
};

struct dbq_cursor {
    bbuint32_t cursordata[4];
};

typedef struct {
    unsigned long long rqid;
    unsigned step;
    uuid_t uuid;
} osqlpf_step;

/* global settings */
extern int gbl_sc_timeoutms;
extern int gbl_trigger_timepart;

extern const char *const gbl_db_release_name;
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
extern int gbl_uses_accesscontrol_tableXnode;

extern int gbl_upd_key;
extern unsigned long long gbl_sqltick;
extern int gbl_nonames;
extern int gbl_abort_on_missing_session;
extern int gbl_reject_osql_mismatch;
extern int gbl_abort_on_clear_inuse_rqid;

extern int gbl_exit_on_pthread_create_fail;
extern int gbl_exit_on_internal_error;

extern int gbl_osql_blockproc_timeout_sec;
extern int gbl_osql_max_throttle_sec;
extern int gbl_throttle_sql_overload_dump_sec;
extern int gbl_toblock_net_throttle;
extern int gbl_heartbeat_check;
extern int gbl_osql_heartbeat_send;
extern int gbl_osql_heartbeat_alert;
extern int gbl_osql_bkoff_netsend_lmt;
extern int gbl_osql_bkoff_netsend;
extern int gbl_osql_max_queue;
extern int gbl_net_poll;
extern int gbl_osql_net_poll;
extern int gbl_osql_net_portmux_register_interval;
extern int gbl_signal_net_portmux_register_interval;
extern int gbl_net_portmux_register_interval;
extern int gbl_net_max_queue;
extern int gbl_nullfkey;
extern int gbl_prefaulthelper_blockops;
extern int gbl_prefaulthelper_sqlreadahead;
extern int gbl_prefaulthelper_tagreadahead;
extern int gbl_maxblockops;
extern int gbl_rangextunit;
extern int gbl_honor_rangextunit_for_old_apis;
extern int gbl_readahead;
extern int gbl_sqlreadahead;
extern int gbl_readaheadthresh;
extern int gbl_sqlreadaheadthresh;
extern int gbl_iothreads;
extern int gbl_ioqueue;
extern int gbl_prefaulthelperthreads;

extern int gbl_osqlpfault_threads;
extern osqlpf_step *gbl_osqlpf_step;
extern queue_type *gbl_osqlpf_stepq;

extern int gbl_starttime;
extern int gbl_enable_bulk_import; /* allow this db to bulk import */
extern int gbl_enable_bulk_import_different_tables;
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
extern char *gbl_mynode;     /* my hostname */
struct in_addr gbl_myaddr;   /* my IPV4 address */
extern char *gbl_myhostname; /* my hostname */
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
extern int gbl_use_fstblk;
extern int gbl_fstblk_vb;
extern int gbl_fstblk_dbgtrans;
extern size_t gbl_fstblk_bucket_sz; /* how many seqnums to drop in a bucket */
extern size_t gbl_fstblk_bucket_gr; /* granularity, 0 for default */
extern size_t gbl_fstblk_minq; /* min no of seqnums to queue before purge */
extern size_t gbl_fstblk_maxq; /* max no of seqnums to queue              */
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
extern history *reqhist;
extern int gbl_sbuftimeout;
extern int sqldbgflag;
extern int gbl_conv_flush_freq;       /* this is currently ignored */
extern int gbl_meta_lite;      /* used at init time to say we prefer lite */
extern int gbl_context_in_key; /* whether to drop find context in last
                                  key found (in dtastripe mode) */
extern int gbl_ready;          /* gets set just before waitft is called
                                  and never gets unset */
extern int gbl_debug_verify_tran;
extern int gbl_queue_debug;
extern unsigned gbl_goose_add_rate;
extern unsigned gbl_goose_consume_rate;
extern int gbl_queue_sleeptime;
extern int gbl_reset_queue_cursor;
extern int gbl_readonly;
extern int gbl_use_bbipc;
extern int gbl_init_single_meta;
extern unsigned long long gbl_sc_genids[MAXDTASTRIPE];
extern int gbl_sc_usleep;
extern int gbl_sc_wrusleep;
extern unsigned gbl_sc_adds;
extern unsigned gbl_sc_updates;
extern unsigned gbl_sc_deletes;
extern long long gbl_sc_nrecs;
extern long long gbl_sc_prev_nrecs;
extern int gbl_sc_last_writer_time;
extern int gbl_default_livesc;
extern int gbl_default_plannedsc;
extern int gbl_default_sc_scanmode;
extern int gbl_sc_abort;
extern int gbl_tranmode;
extern volatile int gbl_dbopen_gen;
extern volatile int gbl_analyze_gen;
extern volatile int gbl_views_gen;
extern volatile int gbl_schema_change_in_progress;
extern int gbl_sc_report_freq;
extern int gbl_thrman_trace;
extern int gbl_move_deadlk_max_attempt;
extern int gbl_lock_conflict_trace;
extern int gbl_enque_flush_interval;
extern int gbl_inflate_log;
extern pthread_attr_t gbl_pthread_attr_detached;
extern unsigned int gbl_nsql;
extern long long gbl_nsql_steps;

extern unsigned int gbl_nnewsql;
extern long long gbl_nnewsql_steps;

extern int gbl_sql_client_stats;

extern int gbl_selectv_rangechk;

extern int gbl_init_with_rowlocks;
extern int gbl_init_with_genid48;
extern int gbl_init_with_odh;
extern int gbl_init_with_ipu;
extern int gbl_init_with_instant_sc;
extern int gbl_init_with_compr;
extern int gbl_init_with_compr_blobs;
extern int gbl_init_with_bthash;

extern int gbl_sqlhistsz;
extern int gbl_force_bad_directory;
extern int gbl_replicate_local;
extern int gbl_replicate_local_concurrent;

extern int gbl_verify_abort;

extern int gbl_sqlrdtimeoutms;
extern int gbl_sqlwrtimeoutms;
extern int gbl_sort_nulls_correctly;

extern int gbl_master_changes;
extern int gbl_sc_commit_count;

extern int gbl_fix_validate_cstr;
extern int gbl_warn_validate_cstr;

extern int gbl_pushlogs_after_sc;
extern int gbl_prefault_verbose;
extern int gbl_ftables;
extern int gbl_check_client_tags;

extern int gbl_max_tables_info;

extern int gbl_prefault_readahead;
extern int gbl_prefault_toblock_bcast;
extern int gbl_prefault_toblock_local;

extern int gbl_appsock_pooling;
extern struct thdpool *gbl_appsock_thdpool;
extern struct thdpool *gbl_sqlengine_thdpool;
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
extern int gbl_dump_queues_on_exit;

extern int gbl_incoherent_msg_freq;
extern int gbl_incoherent_alarm_time;
extern int gbl_max_incoherent_nodes;

extern int n_retries_transaction_active;
extern int n_retries_transaction_done;

extern int gbl_disable_deadlock_trace;
extern int gbl_enable_pageorder_trace;
extern int gbl_disable_overflow_page_trace;
extern int gbl_simulate_rowlock_deadlock_interval;
extern int gbl_debug_rowlocks;

extern int gbl_max_columns_soft_limit;

extern int gbl_use_plan;

extern int gbl_instant_schema_change;

extern int gbl_num_record_converts;
extern int gbl_num_record_upgrades;

extern int gbl_enable_sql_stmt_caching;

extern int gbl_sql_pool_emergency_queuing_max;

extern int gbl_verify_rep_log_records;
extern int gbl_enable_osql_logging;
extern int gbl_enable_osql_longreq_logging;

extern int gbl_osql_verify_ext_chk;

extern int gbl_genid_cache;

extern int gbl_max_appsock_connections;

extern int gbl_master_changed_oldfiles;
extern int gbl_use_bbipc_global_fastseed;
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

/* init routines */
int appsock_init(void);
int thd_init(void);
void sqlinit(void);
void sqlnet_init(void);
int sqlpool_init(void);
int schema_init(void);
int osqlpfthdpool_init(void);
void toblock_init(void);

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
void appsock_handler_start(struct dbenv *dbenv, SBUF2 *sb);
void appsock_coalesce(struct dbenv *dbenv);
void close_appsock(SBUF2 *sb);
void thd_stats(void);
void thd_dbinfo2_stats(struct db_info2_stats *stats);
void thd_coalesce(struct dbenv *dbenv);
void unlock_swapin(void);
char *getorigin(struct ireq *iq);
void thd_dump(void);

enum comdb2_queue_types {
    REQ_WAITFT = 0,
    REQ_SOCKET,
    REQ_OFFLOAD,
    REQ_SOCKREQUEST,
    REQ_PQREQUEST
};

int convert_client_ftype(int type);

int handle_buf(struct dbenv *dbenv, uint8_t *p_buf, const uint8_t *p_buf_end,
               int debug, char *frommach); /* 040307dh: 64bits */
int handle_buf_offload(struct dbenv *dbenv, uint8_t *p_buf,
                       const uint8_t *p_buf_end, int debug, char *frommach,
                       sorese_info_t *sorese);
int handle_buf_sorese(struct dbenv *dbenv, struct ireq *iq, int debug);
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

struct buf_lock_t {
    pthread_mutex_t req_lock;
    pthread_cond_t wait_cond;
    int rc;
    int reply_done;
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
void hexdumpdta(unsigned char *p, int len);
const char *req2a(int opcode);
void reqerrstr(struct ireq *iq, int rc, char *format, ...);
void reqerrstrhdr(struct ireq *iq, char *format,
                  ...); /* keep an error header in a concatenated manner */
void reqerrstrclr(struct ireq *iq);
void reqerrstrhdrclr(struct ireq *iq); /* clear error header */

/* internal request forwarding */
int ireq_forward_to_master(struct ireq *iq, int len);

int getkeyrecnums(const struct dbtable *db, int ixnum);
int getkeysize(const struct dbtable *db, int ixnum); /* get key size of db */
int getdatsize(const struct dbtable *db);            /* get data size of db*/
int getdefaultdatsize(const struct dbtable *db);
int getondiskclientdatsize(const struct dbtable *db);
int getclientdatsize(const struct dbtable *db, char *sname);

struct dbtable *getdbbynum(int num);           /*look up managed db's by number*/
struct dbtable *get_dbtable_by_name(const char *name); /*look up managed db's by name*/
struct dbtable *
getfdbbyname(const char *name); /*look up managed foreign db's by name*/
struct dbtable *getfdbbynameenv(
    struct dbenv *dbenv,
    const char *name); /* look up foreign db by name with given env */
struct dbtable *
getqueuebyname(const char *name); /*look up managed queue db's by name*/
sequence_t *getsequencebyname(const char *name);
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
void backend_update_sync(struct dbenv *dbenv);
void backend_sync_stat(struct dbenv *dbenv);

void init_fake_ireq_auxdb(struct dbenv *dbenv, struct ireq *iq, int auxdb);
void init_fake_ireq(struct dbenv *, struct ireq *);
int set_tran_lowpri(struct ireq *iq, tran_type *tran);

/* long transaction routines */

int purge_expired_long_transactions(struct dbenv *dbenv);
int add_new_transaction_entry(struct dbenv *dbenv, void *entry);
void tran_dump(struct long_trn_stat *tstats);

/* transactional stuff */
int trans_start(struct ireq *, tran_type *parent, tran_type **out);
int trans_start_sc(struct ireq *, tran_type *parent, tran_type **out);
int trans_start_set_retries(struct ireq *, tran_type *parent, tran_type **out,
                            int retries);
int trans_start_logical(struct ireq *, tran_type **out);
int trans_start_logical_sc(struct ireq *, tran_type **out);
int is_rowlocks_transaction(tran_type *);
int rowlocks_check_commit_physical(bdb_state_type *, tran_type *,
                                   int blockop_count);
tran_type *trans_start_readcommitted(struct ireq *, int trak);
tran_type *trans_start_serializable(struct ireq *, int trak, int epoch,
                                    int file, int offset, int *error);
tran_type *trans_start_snapisol(struct ireq *, int trak, int epoch, int file,
                                int offset, int *error);
tran_type *trans_start_socksql(struct ireq *, int trak);
int trans_commit(struct ireq *iq, void *trans, char *source_host);
int trans_commit_seqnum(struct ireq *iq, void *trans, db_seqnum_type *seqnum);
int trans_commit_adaptive(struct ireq *iq, void *trans, char *source_host);
int trans_commit_logical(struct ireq *iq, void *trans, char *source_host,
                         int timeoutms, int adaptive, void *blkseq, int blklen,
                         void *blkkey, int blkkeylen);
int trans_abort(struct ireq *iq, void *trans);
int trans_abort_priority(struct ireq *iq, void *trans, int *priority);
int trans_abort_logical(struct ireq *iq, void *trans, void *blkseq, int blklen,
                        void *blkkey, int blkkeylen);
int trans_wait_for_seqnum(struct ireq *iq, char *source_host,
                          db_seqnum_type *ss);
int trans_wait_for_last_seqnum(struct ireq *iq, char *source_host);

/* find context for pseudo-stable cursors */
int get_context(struct ireq *iq, unsigned long long *context);
int cmp_context(struct ireq *iq, unsigned long long genid,
                unsigned long long context);

/* for fast load.. */
int load_record(struct dbtable *db, void *buf);
void load_data_done(struct dbtable *db);

/*index routines*/
int ix_addk(struct ireq *iq, void *trans, void *key, int ixnum,
            unsigned long long genid, int rrn, void *dta, int dtalen);
int ix_addk_auxdb(int auxdb, struct ireq *iq, void *trans, void *key, int ixnum,
                  unsigned long long genid, int rrn, void *dta, int dtalen);
int ix_upd_key(struct ireq *iq, void *trans, void *key, int keylen, int ixnum,
               unsigned long long genid, unsigned long long oldgenid, void *dta,
               int dtalen);

int ix_delk(struct ireq *iq, void *trans, void *key, int ixnum, int rrn,
            unsigned long long genid);
int ix_delk_auxdb(int auxdb, struct ireq *iq, void *trans, void *key, int ixnum,
                  int rrn, unsigned long long genid);

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
                   int blobno, int rrn, unsigned long long newgenid);
int blob_no_upd_auxdb(int auxdb, struct ireq *iq, void *trans, int rrn,
                      unsigned long long oldgenid, unsigned long long newgenid,
                      int blobmap);
int blob_upv(struct ireq *iq, void *trans, int vptr,
             unsigned long long oldgenid, void *newdta, int newlen, int blobno,
             int rrn, unsigned long long newgenid);
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
             size_t length, int rrn, unsigned long long genid);
int blob_add_auxdb(int auxdb, struct ireq *iq, void *trans, int blobno,
                   void *data, size_t length, int rrn,
                   unsigned long long genid);

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

int check_table_schema(struct dbenv *dbenv, const char *table,
                       const char *csc2file);

struct dbtable *find_table(const char *table);
int bt_hash_table(char *table, int szkb);
int del_bt_hash_table(char *table);
int stat_bt_hash_table(char *table);
int stat_bt_hash_table_reset(char *table);
int fastinit_table(struct dbenv *dbenvin, char *table);
int add_cmacc_stmt(struct dbtable *db, int alt);
int add_cmacc_stmt_no_side_effects(struct dbtable *db, int alt);

void cleanup_newdb(struct dbtable *);
struct dbtable *newdb_from_schema(struct dbenv *env, char *tblname, char *fname,
                             int dbnum, int dbix, int is_foreign);
struct dbtable *newqdb(struct dbenv *env, const char *name, int avgsz, int pagesize,
                  int isqueuedb);
int add_queue_to_environment(char *table, int avgitemsz, int pagesize);
void stop_threads(struct dbenv *env);
void resume_threads(struct dbenv *env);
void replace_db_idx(struct dbtable *p_db, int idx);
int reload_schema(char *table, const char *csc2, tran_type *tran);
void delete_db(char *db_name);
int ix_find_rnum_by_recnum(struct ireq *iq, int recnum_in, int ixnum,
                           void *fndkey, int *fndrrn, unsigned long long *genid,
                           void *fnddta, int *fndlen, int *recnum, int maxlen);
int get_schema_version(const char *table);
int put_schema_version(const char *table, void *tran, int version);

sequence_t *new_sequence(char *name, long long min_val, long long max_val,
                         long long increment, bool cycle, long long start_val,
                         long long chunk_size, char flags,
                         long long next_start_val);
void cleanup_sequence(sequence_t *seq);
void remove_sequence_ranges(sequence_t *seq);

int put_db_odh(struct dbtable *db, tran_type *, int odh);
int get_db_odh(struct dbtable *db, int *odh);
int get_db_odh_tran(struct dbtable *, int *odh, tran_type *);
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

int load_new_table_schema_file(struct dbenv *dbenv, const char *table,
                               const char *csc2file);
int load_new_table_schema_file_trans(void *tran, struct dbenv *dbenv,
                                     const char *table, const char *csc2file);
int load_new_table_schema_tran(struct dbenv *dbenv, tran_type *tran,
                               const char *table, const char *csc2_text);
int load_new_table_schema(struct dbenv *dbenv, const char *table,
                          const char *csc2_text);
int load_new_table_schema_trans(void *tran, struct dbenv *dbenv,
                                const char *table, const char *csc2_text);
int dump_all_csc2_to_disk();
int dump_table_csc2_to_disk_fname(struct dbtable *db, const char *csc2_fname);
int dump_table_csc2_to_disk(const char *table);
int get_csc2_fname(const struct dbtable *db, const char *dir, char *fname,
                   size_t fname_len);
int get_generic_csc2_fname(const struct dbtable *db, char *fname, size_t fname_len);

void flush_db(void);
int compare_all_tags(const char *table, FILE *out);
int restore_constraint_pointers(struct dbtable *db, struct dbtable *newdb);
int backout_constraint_pointers(struct dbtable *db, struct dbtable *newdb);
int populate_reverse_constraints(struct dbtable *db);
int has_index_changed(struct dbtable *db, char *keynm, int ct_check, int newkey,
                      FILE *out, int accept_type_change);
int appsock_schema_change(SBUF2 *sb, int *keepsocket);
int appsock_repopnewlrl(SBUF2 *sb, int *keepsocket);
int appsock_bulk_import(SBUF2 *sb, int *keepsocket);
int appsock_bulk_import_foreign(SBUF2 *sb, int *keepsocket, int version);
int resume_schema_change(void);
int bulk_import(const char *tablename, const bulk_import_data_t *p_foreign_data,
                const char *p_foreign_dbmach, SBUF2 *foreign_sb);

void debug_trap(char *line, int lline);
int reinit_db(struct dbtable *db);
int truncate_db(struct dbtable *db);
int count_db(struct dbtable *db);
int compact_db(struct dbtable *db, int timeout, int freefs);
int ix_find_last_dup_rnum_kl(struct ireq *iq, int ixnum, void *key, int keylen,
                             void *fndkey, int *fndrrn,
                             unsigned long long *genid, void *fnddta,
                             int *fndlen, int *recnum, int maxlen);
int ix_find_rnum_kl(struct ireq *iq, int ixnum, void *key, int keylen,
                    void *fndkey, int *fndrrn, unsigned long long *genid,
                    void *fnddta, int *fndlen, int *recnum, int maxlen);
int ix_find_rnum_by_recnum_kl(struct ireq *iq, int recnum_in, int ixnum,
                              void *fndkey, int *fndrrn,
                              unsigned long long *genid, void *fnddta,
                              int *fndlen, int *recnum, int maxlen);
int ix_next_rnum_kl(struct ireq *iq, int ixnum, void *key, int keylen,
                    void *last, int lastrrn, unsigned long long lastgenid,
                    void *fndkey, int *fndrrn, unsigned long long *genid,
                    void *fnddta, int *fndlen, int *recnum, int maxlen);
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
int broadcast_quiesce_threads(void);
int broadcast_resume_threads(void);
int broadcast_close_db(char *table);
int broadcast_close_only_db(char *table);
int broadcast_morestripe_and_open_all_dbs(int newdtastripe, int newblobstripe);
int broadcast_close_all_dbs(void);
int broadcast_sc_end(uint64_t seed);
int broadcast_sc_start(uint64_t seed, uint32_t host, time_t t);
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
int dbq_add(struct ireq *iq, void *trans, const void *dta, size_t dtalen);
int dbq_consume(struct ireq *iq, void *trans, int consumer, const void *fnd);
int dbq_consume_genid(struct ireq *, void *trans, int consumer, const genid_t);
int dbq_get(struct ireq *iq, int consumer, const struct dbq_cursor *prevcursor,
            void **fnddta, size_t *fnddtalen, size_t *fnddtaoff,
            struct dbq_cursor *fndcursor, unsigned int *epoch);
void dbq_get_item_info(const void *fnd, size_t *dtaoff, size_t *dtalen);
unsigned long long dbq_item_genid(const void *dta);
typedef int (*dbq_walk_callback_t)(int consumern, size_t item_length,
                                   unsigned int epoch, void *userptr);
int dbq_walk(struct ireq *iq, int flags, dbq_walk_callback_t callback,
             void *userptr);
int dbq_dump(struct dbtable *db, FILE *out);
int fix_consumers_with_bdblib(struct dbenv *dbenv);
int dbq_add_goose(struct ireq *iq, void *trans);
int dbq_check_goose(struct ireq *iq, void *trans);
int dbq_consume_goose(struct ireq *iq, void *trans);

/* sql stuff */
int create_sqlmaster_records(void *tran);
void form_new_style_name(char *namebuf, int len, struct schema *schema,
                         const char *csctag, const char *dbname);

void get_copy_rootpages_nolock(struct sql_thread *thd);
void get_copy_rootpages(struct sql_thread *thd);
void free_copy_rootpages(struct sql_thread *thd);
void create_master_tables(void);
int new_indexes_syntax_check(struct ireq *iq);
void handle_isql(struct dbtable *db, SBUF2 *sb);
void handle_timesql(SBUF2 *sb, struct dbtable *db);
int handle_fastsql(struct thr_handle *thr_self, SBUF2 *sb, struct dbtable *db,
                   int usepool, int *keepsock);
int handle_sql(SBUF2 *sb);
int handle_llops(SBUF2 *sb, struct dbenv *dbenv);
void sql_dump_running_statements(void);
char *stradd(char **s1, char *s2, int freeit);
void dbgtrace(int, char *, ...);

int get_sqlite_entry_size(int n);
void *get_sqlite_entry(int n);
void get_sqlite_tblnum_and_ixnum(struct sql_thread *thd, int iTable,
                                 int *tblnum, int *ixnum);

int schema_var_size(struct schema *sc);

/* request handlers */
int handle_ireq(struct ireq *iq);
int toblock(struct ireq *iq);
int todbinfo(struct ireq *iq);
int todbinfo2(struct ireq *iq);
int todescribe(struct ireq *iq);
int tofind(struct ireq *iq);
int tofind2(struct ireq *iq);
int tofind2kl(struct ireq *iq);
int toformkey(struct ireq *iq);
int tolongblock(struct ireq *iq);
int tonext(struct ireq *iq);
int tonext2(struct ireq *iq);
int tonext2kl(struct ireq *iq);
int torngext2(struct ireq *iq);
int torngextp2(struct ireq *iq);
int torngexttag(struct ireq *iq);
int torngexttagp(struct ireq *iq);
int torngexttagtz(struct ireq *iq);
int torngexttagptz(struct ireq *iq);
int tooldfindrrn(struct ireq *iq);
int tofindrrn(struct ireq *iq);
int tonumrrn(struct ireq *iq);
int tohighrrn(struct ireq *iq);
int tocount(struct ireq *iq);
int tostored(struct ireq *iq);
int tomsgtrap(struct ireq *iq);
int todescribekeys(struct ireq *iq);
int togetkeynames(struct ireq *iq);
int toclear(struct ireq *iq);
int tofastinit(struct ireq *iq);
int torngextx(struct ireq *iq);
void count_table_in_thread(const char *table);
void handle_explain(SBUF2 *sb, int trace, int all);
void handle_rrsql(SBUF2 *sb);
void handle_reinit(SBUF2 *sb, struct dbenv *dbenv);
int totran(struct ireq *iq, int op);
int tolockget(struct ireq *iq);
int toproxconfig(struct ireq *iq);
int tocoherentchange(struct ireq *iq);
int tomakeincoherent(struct ireq *iq);
int findkl_enable_blob_verify(void);

void sltdbt_get_stats(int *n_reqs, int *l_reqs);

void dbghexdump(int flag, void *memp, size_t len);
void hash_set_hashfunc(hash_t *h, hashfunc_t hashfunc);
void hash_set_cmpfunc(hash_t *h, cmpfunc_t cmpfunc);

enum mach_class get_my_mach_class(void);
enum mach_class get_mach_class(const char *host);
const char *get_mach_class_str(char *host);
const char *get_class_str(enum mach_class cls);
int allow_write_from_remote(const char *host);
int allow_cluster_from_remote(const char *host);
int allow_broadcast_to_remote(const char *host);
int process_allow_command(char *line, int lline);

/* blob caching to support find requests */
int gather_blob_data(struct ireq *iq, const char *tag, blob_status_t *b,
                     const char *to_tag);
int gather_blob_data_byname(const char *dbname, const char *tag,
                            blob_status_t *b);
int check_one_blob_consistency(struct ireq *iq, const char *table,
                               const char *tag, blob_status_t *b, void *record,
                               int blob_index, int cblob);
int check_blob_consistency(struct ireq *iq, const char *table, const char *tag,
                           blob_status_t *b, const void *record);
int check_and_repair_blob_consistency(struct ireq *iq, const char *table,
                                      const char *tag, blob_status_t *b,
                                      const void *record);
void free_blob_status_data(blob_status_t *b);
void free_blob_status_data_noreset(blob_status_t *b);
int cache_blob_data(struct ireq *iq, int rrn, unsigned long long genid,
                    const char *table, const char *tag, unsigned *extra1,
                    unsigned *extra2, int numblobs, size_t *bloblens,
                    size_t *bloboffs, void **blobptrs, size_t total_length);
int init_blob_cache(void);
int toblobask(struct ireq *iq);
void blob_print_stats(void);
void purge_old_cached_blobs(void);

void commit_schemas(const char *tblname);
struct schema *new_dynamic_schema(const char *s, int len, int trace);
void free_dynamic_schema(const char *table, struct schema *dsc);
int getdefaultkeysize(const struct dbtable *db, int ixnum);
int getdefaultdatsize(const struct dbtable *db);
int update_sqlite_stats(struct ireq *iq, void *trans, void *dta);
void *do_verify(void *);
void dump_tagged_buf(const char *table, const char *tag,
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
uint64_t calc_table_size(struct dbtable *db);
uint64_t calc_table_size_analyze(struct dbtable *db);

enum { WHOLE_BUFFER = -1 };

void diagnostics_dump_rrn(struct dbtable *tbl, int rrn);
void diagnostics_dump_dta(struct dbtable *db, int dtanum);

/* queue stuff */
void dbqueue_coalesce(struct dbenv *dbenv);
void dbqueue_admin(struct dbenv *dbenv);
int dbqueue_add_consumer(struct dbtable *db, int consumer, const char *method,
                         int noremove);
int dbqueue_set_consumern_options(struct dbtable *db, int consumer,
                                  const char *opts);
int dbqueue_set_consumer_options(struct consumer *consumer, const char *opts);
void dbqueue_stat(struct dbtable *db, int fullstat, int walk_queue, int blocking);
void dbqueue_flush_in_thread(struct dbtable *db, int consumern);
void dbqueue_flush_abort(void);
int consumer_change(const char *queuename, int consumern, const char *method);
void dbqueue_wake_all_consumers(struct dbtable *db, int force);
void dbqueue_wake_all_consumers_all_queues(struct dbenv *dbenv, int force);
void dbqueue_goose(struct dbtable *db, int force);
void dbqueue_stop_consumers(struct dbtable *db);
void dbqueue_restart_consumers(struct dbtable *db);
int dbqueue_check_consumer(const char *method);

/* Resource manager */
void initresourceman(const char *newlrlname);
char *getdbrelpath(const char *relpath);
void addresource(const char *name, const char *filepath);
const char *getresourcepath(const char *name);
void dumpresources(void);

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
    RECFLAGS_NO_TRIGGERS = 1,
    /* don't use constraints or defer index operations */
    RECFLAGS_NO_CONSTRAINTS = 2,
    /* if the schema is not dynamic then bzero the nulls map */
    RECFLAGS_DYNSCHEMA_NULLS_ONLY = 4,
    /* called from update cascade code, affects key operations */
    UPDFLAGS_CASCADE = 8,
    /* use .NEW..ONDISK rather than .ONDISK */
    RECFLAGS_NEW_SCHEMA = 16,
    /* use input genid if in dtastripe mode */
    RECFLAGS_KEEP_GENID = 32,
    /* don't add blobs and ignore blob buffers - used in planned schema
     * changes since we don't touch blob files. */
    RECFLAGS_NO_BLOBS = 64,
    /* Used for block/sock/offsql updates to indicate that if a blob is not
     * provided then it should be NULLed out.  In this mode all blobs for
     * the record that are non-NULL will be given. */
    RECFLAGS_DONT_SKIP_BLOBS = 128,
    RECFLAGS_ADD_FROM_SC = 256,
    /* used for upgrade record */
    RECFLAGS_UPGRADE_RECORD = RECFLAGS_DYNSCHEMA_NULLS_ONLY |
                              RECFLAGS_KEEP_GENID | RECFLAGS_NO_TRIGGERS |
                              RECFLAGS_NO_CONSTRAINTS | RECFLAGS_NO_BLOBS | 512
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
               unsigned long long ins_keys, int opcode, int blkpos, int flags);

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
                   blob_buffer_t *idx_blobs);

int upd_new_record(struct ireq *iq, void *trans, unsigned long long oldgenid,
                   const void *old_dta, unsigned long long newgenid,
                   const void *new_dta, unsigned long long ins_keys,
                   unsigned long long del_keys, int nd_len, const int *updCols,
                   blob_buffer_t *blobs, int deferredAdd,
                   blob_buffer_t *del_idx_blobs, blob_buffer_t *add_idx_blobs);

int upd_new_record_add2indices(struct ireq *iq, void *trans,
                               unsigned long long newgenid, const void *new_dta,
                               int nd_len, unsigned long long ins_keys,
                               int use_new_tag, blob_buffer_t *blobs);

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

enum { LOG_DEL_ABS_ON, LOG_DEL_ABS_OFF, LOG_DEL_REFRESH };
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

long long get_unique_longlong(struct dbenv *env);
void no_new_requests(struct dbenv *dbenv);

int get_next_seqno(void *tran, long long *seqno);
int add_oplog_entry(struct ireq *iq, void *trans, int type, void *logrec,
                    int logsz);
int local_replicant_write_clear(struct dbtable *db);
long long get_record_unique_id(struct dbtable *db, void *rec);
void cancel_sql_statement(int id);
void cancel_sql_statement_with_cnonce(const char *cnonce);

struct client_query_stats *get_query_stats_from_thd();

/* reqlog.c - new logging stuff */

enum {
    REQL_INFO = 1,    /* info on the request being processed */
    REQL_TRACE = 2,   /* basic trace */
    REQL_RESULTS = 4, /* query results */
    REQL_QUERY = 8    /* display only the query */
};

enum { REQL_BAD_CSTR_FLAG = 1 };

int reqlog_init(const char *dbname);
void reqlog_process_message(char *line, int st, int lline);
void reqlog_stat(void);
void reqlog_help(void);
void reqlog_free(struct reqlogger *reqlogger);
void reqlog_reset_logger(struct reqlogger *logger);
int reqlog_pushprefixv(struct reqlogger *logger, const char *format,
                       va_list args);
int reqlog_pushprefixf(struct reqlogger *logger, const char *format, ...);
int reqlog_popallprefixes(struct reqlogger *logger);
int reqlog_popprefix(struct reqlogger *logger);
int reqlog_logv(struct reqlogger *logger, unsigned event_flag, const char *fmt,
                va_list args);
int reqlog_logf(struct reqlogger *logger, unsigned event_flag, const char *fmt,
                ...);
int reqlog_logl(struct reqlogger *logger, unsigned event_flag, const char *s);
int reqlog_loghex(struct reqlogger *logger, unsigned event_flag, const void *d,
                  size_t len);
void reqlog_set_cost(struct reqlogger *logger, double cost);
void reqlog_set_rows(struct reqlogger *logger, int rows);
void reqlog_usetable(struct reqlogger *logger, const char *tablename);
void reqlog_setflag(struct reqlogger *logger, unsigned flag);
int reqlog_logl(struct reqlogger *logger, unsigned event_flag, const char *s);
void reqlog_new_request(struct ireq *iq);
void reqlog_new_sql_request(struct reqlogger *logger, char *sqlstmt);
void reqlog_set_sql(struct reqlogger *logger, char *sqlstmt);
uint64_t reqlog_current_us(struct reqlogger *logger);
void reqlog_end_request(struct reqlogger *logger, int rc, const char *callfunc, int line);
void reqlog_diffstat_init(struct reqlogger *logger);
/* this is meant to be called by only 1 thread, will need locking if
 * more than one threads were to be involved */
void reqlog_diffstat_dump(struct reqlogger *logger);
int reqlog_diffstat_thresh();
void reqlog_set_diffstat_thresh(int val);
int reqlog_truncate();
void reqlog_set_truncate(int val);
void reqlog_set_vreplays(struct reqlogger *logger, int replays);
void reqlog_set_queue_time(struct reqlogger *logger, uint64_t timeus);
void reqlog_set_fingerprint(struct reqlogger *logger, char fingerprint[16]);
void reqlog_set_rqid(struct reqlogger *logger, void *id, int idlen);
void reqlog_set_request(struct reqlogger *logger, CDB2SQLQUERY *q);
void reqlog_set_event(struct reqlogger *logger, const char *evtype);
void reqlog_add_table(struct reqlogger *logger, const char *table);
void reqlog_set_error(struct reqlogger *logger, const char *error);
void reqlog_set_path(struct reqlogger *logger, struct client_query_stats *path);
void reqlog_set_context(struct reqlogger *logger, int ncontext, char **context);

void eventlog_params(struct reqlogger *logger, sqlite3_stmt *stmt,
                     struct schema *params, struct sqlclntstate *clnt);

void process_nodestats(void);
void nodestats_report(FILE *fh, const char *prefix, int disp_rates);
void nodestats_node_report(FILE *fh, const char *prefix, int disp_rates,
                           char *host);
struct rawnodestats *get_raw_node_stats(char *host);

struct reqlogger *reqlog_alloc(void);
int peer_dropped_connection(struct sqlclntstate *clnt);

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

enum rsptype { RSP_COORDINATOR_PARTIAL_BLOCK = 12 };

struct crsphdr {
    enum rsptype rsptype;
};

/* Wrappers around berkeley lock code (gut says bad idea, brain insists) */
int locks_get_locker(unsigned int *lid);
int locks_lock_row(unsigned int lid, unsigned long long genid);
int locks_release_locker(unsigned int lid);

void reload_gtids(void);

int handle_coordinator_master_switch(void);

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

int remaining_active_gtid_count(void);

void check_old_transaction_status(void);

/* TODO: move all the twophase stuff into a separate .h file - too much
 * pollution */

struct active_tid_list_member {
    tranid_t id;
    int dbnum;
    LINKC_T(struct active_tid_list) lnk;
};

typedef LISTC_T(struct active_tid_list) active_tid_list;

void get_active_tranid_list(active_tid_list *l);
void twophase_process_message(char *line, int lline, int st);
void get_instant_record_lock(unsigned long long genid);
int genid_exists(struct ireq *iq, unsigned long long genid);
extern int gbl_exclusive_blockop_qconsume;
extern pthread_rwlock_t gbl_block_qconsume_lock;

struct genid_list {
    unsigned long long genid;
    LINKC_T(struct genid_list) lnk;
};

typedef LISTC_T(struct genid_list) genid_list_type;

struct ptrans {
    tranid_t tid;
    int dbnum;
    int start_time;
    void *parent_trans;
    genid_list_type modified_genids;
    int done;
    int waiting_for_commit;
    struct ireq *iq; /* request/thread handling this transaction */
};

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

void mtrap_init(void);
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
                           const uint8_t *p_buf_end);

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

char *osql_get_tran_summary(struct ireq *iq);
char *osql_sorese_type_to_str(int stype);
extern int gbl_malloc_regions;

void berkdb_use_malloc_for_regions_with_callbacks(void *mem,
                                                  void *(*alloc)(void *, int),
                                                  void (*free)(void *, void *));

extern int gbl_rowlocks;
extern int gbl_disable_tagged_api;
extern int gbl_snapisol;
extern int gbl_new_snapisol;
extern int gbl_new_snapisol_logging;
extern int gbl_new_snapisol_asof;
extern int gbl_update_shadows_interval;
extern int gbl_lowpri_snapisol_sessions;

/* stats */
/* non-sql request service times (last minute, last hour, since start) */
struct quantize *q_min;
struct quantize *q_hour;
struct quantize *q_all;
/* sql query times */
struct quantize *q_sql_min;
struct quantize *q_sql_hour;
struct quantize *q_sql_all;
/* sql #steps */
struct quantize *q_sql_steps_min;
struct quantize *q_sql_steps_hour;
struct quantize *q_sql_steps_all;

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

int net_get_my_port(netinfo_type *netinfo_ptr);

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

struct field *convert_client_field(CDB2SQLQUERY__Bindvalue *bindvalue,
                                   struct field *c_fld);
int bind_parameters(sqlite3_stmt *stmt, struct schema *params,
                    struct sqlclntstate *clnt, char **err);
void bind_verify_indexes_query(sqlite3_stmt *stmt, void *sm);
int verify_indexes_column_value(sqlite3_stmt *stmt, void *sm);

void verify_schema_change_constraint(struct ireq *iq, struct dbtable *, void *trans,
                                     void *od_dta, unsigned long long ins_keys);

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

int vtag_to_ondisk(struct dbtable *db, uint8_t *rec, int *len, uint8_t ver,
                   unsigned long long genid);
int vtag_to_ondisk_vermap(struct dbtable *db, uint8_t *rec, int *len, uint8_t ver);

int get_origin_mach(char *origin);
void comdb2_die(int abort);
int access_control_check_read(struct ireq *iq, tran_type *tran, int *bdberr);
int access_control_check_write(struct ireq *iq, tran_type *tran, int *bdberr);

/* tcm test case enums */
enum { TCM_PARENT_DEADLOCK = 1, TCM_MAX = 1 };

/* return non-zero if this test is enabled */
int tcm_testpoint(int tcmtestid);

/* tagged api big or little endian. */
enum { TAGGED_API_BIG_ENDIAN = 1, TAGGED_API_LITTLE_ENDIAN = 2 };

extern int gbl_check_schema_change_permissions;

extern int gbl_support_datetime_in_triggers;

extern int gbl_use_block_mode_status_code;

extern int gbl_fk_allow_prefix_keys;
extern int gbl_fk_allow_superset_keys;
extern long long gbl_converted_blocksql_requests;
extern int gbl_sql_tranlevel_default;
extern int gbl_sql_tranlevel_preserved;

int io_override_set_std(FILE *f);
FILE *io_override_get_std(void);

void reqlog_set_origin(struct reqlogger *logger, const char *fmt, ...);
const char *reqlog_get_origin(struct reqlogger *logger);
void berkdb_iopool_process_message(char *line, int lline, int st);

uint8_t *comdb2_field_type_put(const comdb2_field_type *field, uint8_t *p_buf,
                               const uint8_t *p_buf_end);

uint8_t *db_info2_iostats_put(const struct db_info2_iostats *p_iostats,
                              uint8_t *p_buf, const uint8_t *p_buf_end);

extern int gbl_log_fstsnd_triggers;

void clear_bulk_import_data(bulk_import_data_t *p_data);

int init_ireq(struct dbenv *dbenv, struct ireq *iq, SBUF2 *sb, uint8_t *p_buf,
              const uint8_t *p_buf_end, int debug, char *frommach, int frompid,
              char *fromtask, int qtype, void *data_hndl, int do_inline,
              int luxref, unsigned long long rqid);
struct ireq *create_sorese_ireq(struct dbenv *dbenv, SBUF2 *sb, uint8_t *p_buf,
                                const uint8_t *p_buf_end, int debug,
                                char *frommach, sorese_info_t *sorese);
void destroy_ireq(struct dbenv *dbenv, struct ireq *iq);

int toclientstats(struct ireq *);

void create_watchdog_thread(struct dbenv *);

int get_max_reclen(struct dbenv *);

void testrep(int niter, int recsz);
int sc_request_disallowed(SBUF2 *sb);
int dump_spfile(char *path, const char *dblrl, char *file_name);
int read_spfile(char *file);

struct bdb_cursor_ifn;
int recover_deadlock(bdb_state_type *, struct sql_thread *,
                     struct bdb_cursor_ifn *, int sleepms);
int recover_deadlock_silent(bdb_state_type *, struct sql_thread *,
                            struct bdb_cursor_ifn *, int sleepms);
int pause_pagelock_cursors(void *arg);
int count_pagelock_cursors(void *arg);
int compare_indexes(const char *table, FILE *out);
void freeschema(struct schema *schema);
void freedb(struct dbtable *db);

extern int gbl_upgrade_blocksql_to_socksql;

extern int gbl_parallel_recovery_threads;

extern int gbl_core_on_sparse_file;
extern int gbl_check_sparse_files;

extern int __slow_memp_fget_ns;
extern int __slow_read_ns;
extern int __slow_write_ns;

#include "dbglog.h"

void handle_testcompr(SBUF2 *sb, const char *table);
void handle_setcompr(SBUF2 *);
void handle_partition(SBUF2 *);
void handle_rowlocks_enable(SBUF2 *);
void handle_rowlocks_enable_master_only(SBUF2 *);
void handle_rowlocks_disable(SBUF2 *);
void handle_genid48_enable(SBUF2 *);
void handle_genid48_disable(SBUF2 *);

extern int gbl_bbipc_slotidx;

extern int gbl_decimal_rounding;
extern int gbl_report_sqlite_numeric_conversion_errors;

extern int dfp_conv_check_status(void *pctx, char *from, char *to);

void fix_constraint_pointers(struct dbtable *db, struct dbtable *newdb);
struct schema *create_version_schema(char *csc2, int version, struct dbenv *);
void set_odh_options(struct dbtable *);
void set_odh_options_tran(struct dbtable *db, tran_type *tran);
void transfer_db_settings(struct dbtable *olddb, struct dbtable *newdb);
int reload_after_bulkimport(struct dbtable *, tran_type *);
int reload_db_tran(struct dbtable *, tran_type *);
int debug_this_request(int until);

int gbl_disable_stable_for_ipu;

extern int gbl_debug_memp_alloc_size;

extern int gbl_max_sql_hint_cache;

/* Remote cursor support */
/* use portmux to open an SBUF2 to local db or proxied db */
SBUF2 *connect_remote_db(const char *dbname, const char *service, char *host);
int get_rootpage_numbers(int nums);

void sql_dump_hints(void);

extern int gbl_disable_exit_on_thread_error;

void sc_del_unused_files(struct dbtable *db);
void sc_del_unused_files_tran(struct dbtable *db, tran_type *tran);

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

extern int gbl_debug_temptables;

extern int gbl_check_sql_source;

extern int gbl_flush_check_active_peer;

extern char *gbl_recovery_options;
extern char *gbl_config_root;

int setup_net_listen_all(struct dbenv *dbenv);

extern int gbl_no_env;

int gbl_hostname_refresh_time;

extern int gbl_noenv_messages;

extern int gbl_bbenv;

extern int gbl_maxblobretries;

extern int gbl_sqlite_sortermult;

int printlog(bdb_state_type *bdb_state, int startfile, int startoff,
             int endfile, int endoff);

void stat4dump(int more, char *table, int istrace);

int net_allow_node(struct netinfo_struct *netinfo_ptr, const char *host);

extern int gbl_ctrace_dbdir;
int gbl_private_blkseq;
int gbl_use_blkseq;

extern int gbl_sc_inco_chk;
extern int gbl_track_queue_time;
extern int gbl_broadcast_check_rmtpol;
extern int gbl_deadlock_policy_override;
extern int gbl_accept_on_child_nets;
extern int gbl_disable_etc_services_lookup;
extern int gbl_sql_random_release_interval;
extern int gbl_debug_queuedb;

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
 * Interface between partition roller and schema change */
int sc_timepart_add_table(const char *existingTableName,
                          const char *newTableName, struct errstat *err);
int sc_timepart_drop_table(const char *tableName, struct errstat *err);

/* SCHEMACHANGE DECLARATIONS*/

// These were all over the place in schemachange, moved here hoping in some
// future refactoring

int compare_tag(const char *table, const char *tag, FILE *out);
int compare_tag_int(struct schema *old, struct schema *new, FILE *out,
                    int strict);
int cmp_index_int(struct schema *oldix, struct schema *newix, char *descr,
                  size_t descrlen);
int getdbidxbyname(const char *p_name);
int open_temp_db_resume(struct dbtable *db, char *prefix, int resume, int temp,
                        tran_type *tran);
int find_constraint(struct dbtable *db, constraint_t *ct);

/* END OF SCHEMACHANGE DECLARATIONS*/

/**
 * Disconnect a socket and tries to save it in sockpool
 */
void disconnect_remote_db(const char *dbname, const char *service, char *host,
                          SBUF2 **psb);

void sbuf2gettimeout(SBUF2 *sb, int *read, int *write);
int sbuf2fread_timeout(char *ptr, int size, int nitems, SBUF2 *sb,
                       int *was_timeout);
int release_locks(const char *trace);

unsigned long long verify_indexes(struct dbtable *db, uint8_t *rec,
                                  blob_buffer_t *blobs, size_t maxblobs,
                                  int is_alter);

/* Authentication types for users */
enum { AUTH_READ = 1, AUTH_WRITE = 2, AUTH_OP = 3, AUTH_USERSCHEMA = 4 };

void check_access_controls(struct dbenv *dbenv);

/* Blob mem. */
extern comdb2bma blobmem; // blobmem for db layer
extern size_t gbl_blobmem_cap;
extern unsigned gbl_blob_sz_thresh_bytes;
extern int gbl_large_str_idx_find;

/* Query fingerprinting */
extern int gbl_fingerprint_queries;

/* Global switch for perfect checkpoint. */
extern int gbl_use_perfect_ckp;
/* (For testing only) Configure how long a checkpoint
   sleeps before asking MPool to flush. */
extern int gbl_ckp_sleep_before_sync;

int set_rowlocks(void *trans, int enable);

/* 0: Return null constraint error for not-null constraint violation on updates
   1: Return conversion error instead */
extern int gbl_upd_null_cstr_return_conv_err;

/* High availability getter & setter */
int get_high_availability(struct sqlclntstate *clnt);
void set_high_availability(struct sqlclntstate *clnt, int val);

/* Update the tunable at runtime. */
comdb2_tunable_err handle_runtime_tunable(const char *name, const char *value);
/* Update the tunable read from lrl file. */
comdb2_tunable_err handle_lrl_tunable(char *name, int name_len, char *value,
                                      int value_len, int flags);

int db_is_stopped(void);

#endif /* !INCLUDED_COMDB2_H */
