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

#ifndef SCHEMACHANGE_H
#define SCHEMACHANGE_H

#include <sbuf2.h>
#include <comdb2.h>
#include <util.h>
#include <locks.h>
#include <list.h>
#include <bdb_schemachange.h>

/* To be forward declared one accessors methods are added */

/* A schema change plan. */
struct scplan {
    int plan_upgrade;
    /* Set to 1 if the plan requires us to call convert_all_records() */
    int plan_convert;

    /* -1 = build from new schema, 0 = use old data file */
    int dta_plan;

    /* Set to 1 if we intend to use the plan for blobs, 0 otherwise.
     * The only reason not to apply the plan to blobs is if we intend
     * to rebuild them with different compression options. */
    int plan_blobs;

    /* 0..MAXBLOBS   => rename old blob file to create this one
     * -1            => build from new schema */
    int blob_plan[MAXBLOBS];

    /* 1 = must keep old blob, 0 = remove old blob file */
    int old_blob_plan[MAXBLOBS];

    /* 0..MAXINDEX  => rename old index file to create this one
     * -1        => build from new schema */
    int ix_plan[MAXINDEX];

    /* 1 = must keep old ix file, 0 = remove old ix file */
    int old_ix_plan[MAXINDEX];
};

struct dest {
    char *dest;
    LINKC_T(struct dest) lnk;
};

/* status for add table process, stored in sc->add_status */
enum add_state { SC_NOT_ADD = 0, SC_TO_ADD = 1, SC_DONE_ADD = 2 };

enum comdb2_partition_type {
    PARTITION_NONE = 0,
    PARTITION_REMOVE = 1,
    PARTITION_MERGE = 2,
    PARTITION_ADD_TIMED = 20,
    PARTITION_ADD_MANUAL = 21,
    PARTITION_ADD_COL_RANGE = 40,
    PARTITION_ADD_COL_HASH = 60,
};

struct comdb2_partition {
    /* Type of the partition */
    enum comdb2_partition_type type;
    union {
        struct timed {
            uint32_t period;
            uint32_t retention;
            uint64_t start;
        } tpt;
        struct merge {
            char tablename[MAXTABLELEN];
            int version;
        } mergetable;
    } u;
};

struct timepart_view;

/* in sync with do_schema_change_if */
enum schema_change_kind {
    SC_INVALID = 0,
    SC_LEGACY_QUEUE = 1,
    SC_LEGACY_MORESTRIPE = 2,
    SC_ADD_QDB_FILE = 3,
    SC_DEL_QDB_FILE = 4,
    SC_ADD_VIEW = 5,
    SC_DROP_VIEW = 6,
    SC_ADDSP = 7,
    SC_DELSP = 8,
    SC_DEFAULTSP = 9,
    SC_SHOWSP = 10,
    SC_ADD_TRIGGER = 11,
    SC_DEL_TRIGGER = 12,
    SC_ADD_SFUNC = 13,
    SC_DEL_SFUNC = 14,
    SC_ADD_AFUNC = 15,
    SC_DEL_AFUNC = 16,
    SC_FULLUPRECS = 17,
    SC_PARTIALUPRECS = 18,
    SC_DROPTABLE = 19,
    SC_TRUNCATETABLE = 20,
    SC_ADDTABLE = 21,
    SC_RENAMETABLE = 22,
    SC_ALIASTABLE = 23,
    SC_ALTERTABLE = 24,
    SC_ALTERTABLE_PENDING = 25,
    SC_REBUILDTABLE = 26,
    SC_ALTERTABLE_INDEX = 27,
    SC_DROPTABLE_INDEX = 28,
    SC_REBUILDTABLE_INDEX = 29,
    SC_LAST /* End marker */
};

#define IS_SC_DBTYPE_TAGGED_TABLE(s) ((s)->kind > SC_DROP_VIEW)
#define IS_FASTINIT(s)                                                         \
    (((s)->kind == SC_DROP_VIEW) || ((s)->kind == SC_TRUNCATETABLE))
#define IS_SFUNC(s) (((s)->kind == SC_ADD_SFUNC) || ((s)->kind == SC_DEL_SFUNC))
#define IS_AFUNC(s) (((s)->kind == SC_ADD_AFUNC) || ((s)->kind == SC_DEL_AFUNC))
#define IS_TRIGGER(s)                                                          \
    (((s)->kind == SC_ADD_TRIGGER) || ((s)->kind == SC_DEL_TRIGGER))
#define IS_UPRECS(s)                                                           \
    (((s)->kind == SC_FULLUPRECS) || ((s)->kind == SC_PARTIALUPRECS))
#define IS_ALTERTABLE(s)                                                       \
    (((s)->kind >= SC_ALTERTABLE) && ((s)->kind <= SC_REBUILDTABLE_INDEX))

struct schema_change_type {
    /*  ==========    persistent members ========== */
    enum schema_change_kind kind;
    unsigned long long rqid;
    uuid_t uuid;
    size_t tablename_len;
    char tablename[MAXTABLELEN];    /* name of table/queue */
    char newtable[MAXTABLELEN];     /* new table name */
    size_t fname_len;
    char fname[256];                /* name of schema file for table schema
                                       change or client provided SP version */
    size_t aname_len;
    char aname[256];         /* advised file name for .csc2 */
    int avgitemsz;           /* average item size for queue creation */
    int newdtastripe;        /* new dtastripe factor */
    int blobstripe;          /* 1 if we are converting to blobstripe */
    int live;
    size_t newcsc2_len;
    char *newcsc2; /* malloced buffer containing the new schema */
    enum convert_scan_mode scanmode;
    int delay_commit;  /* no longer used, leaving for compatibility */
    int force_rebuild; /* force full rebuild of table */
    int force_dta_rebuild;
    int force_blob_rebuild;
    int force; /* force schema change even if not ready */
    int headers; /* Add ondisk headers? -1 for no change*/
    int header_change;
    int compress;       /* new compression algorithm or -1 for no change */
    int compress_blobs; /* new blob com algorithm or -1 for no change */
    int persistent_seq; /* init queue with persistent sequence */
    int ip_updates;     /* inplace updates or -1 for no change */
    int instant_sc;     /* 1 is enable, 0 disable, or -1 for no change */
    int preempted;
    int use_plan;         /* if we want to use a plan so we don't rebuild
                             everything needlessly. */
    int commit_sleep;     /* Used for testing; sleep a bit before committing
                             schemas. */
    int convert_sleep;    /* Also for testing */
    int same_schema;      /* indicates that the schema hasn't changed, so
                             we can skip the schema reload steps */
    int dbnum;
/* instead of failing to resume schemachange, generate sc plan
 * compatible with previous versions of comdb2 depending on which of
 * following flags are set */

#define SC_CHK_PGSZ 0x00000001U
#define SC_IDXRBLD 0x00000002U
#define SC_MASK_FLG 0xfffffffcU /* Detect and fail if newer ver started sc.
                                 * Update this mask when new flags added */
    uint32_t flg;

    uint8_t rebuild_index;    /* option to rebuild only one index */
    uint8_t index_to_rebuild; /* can use just a short for rebuildindex */

    char original_master_node[256];

    LISTC_T(struct dest) dests;

    size_t spname_len;
    char spname[MAX_SPNAME];
    int lua_func_flags; /* lua func flags */

    /* QueueDB operations */
    unsigned long long qdb_file_ver; /* part of file name to add */

    /* ========== runtime members ========== */
    scdone_t done_type;
    int onstack; /* if 1 don't free */
    int nothrevent;
    int already_locked; /* already holding schema lock */
    int keep_locked; /* don't release schema lock upon commit */
    int pagesize; /* pagesize override to use */
    SBUF2 *sb; /* socket to sponsoring program */
    int must_close_sb;
    int use_old_blobs_on_rebuild;
    enum add_state add_state;
    int partialuprecs; /* count updated records in partial table upgrade */

    int resume;           /* if we are trying to resume a schema change,
                           * usually because there is a new master */
    int must_resume;      /* used for partitions, if we generate new shard sc-s upon resume */
    int retry_bad_genids; /* retrying a schema change (with full rebuild)
                             because there are old genids in flight */
    int dryrun;           /* comdb2sc.tsk -y */
    int statistics;       /* comdb2sc.tsk <dbname> stat <table> */
    int use_new_genids;   /* rebuilding old genids needs to
                             get new genids to avoid name collission */
    int finalize;      /* Whether the schema change should be committed */
    int finalize_only; /* only commit the schema change */

    pthread_mutex_t mtx; /* mutex for thread sync */
    pthread_mutex_t mtxStart; /* mutex for thread start */
    pthread_cond_t condStart; /* condition var for thread sync */
    int started;
    int sc_rc;

    struct ireq *iq;
    void *tran; /* transactional schemachange */

    struct schema_change_type *sc_next;

    int usedbtablevers;
    int fix_tp_badvers;

    /* partition */
    const char *timepartition_name; /* time partition name, if any */
    unsigned long long
        timepartition_version; /* time partition tableversion, if any */
    struct comdb2_partition partition;

    /*********************** temporary fields for in progress
     * schemachange************/
    /********************** it will change eventually (do not try to serialize)
     * ************/

    struct dbtable *db;
    struct dbtable *newdb;
    struct timepart_view *newpartition;
    struct scplan plan; /**** TODO This is an abomination, i know. Yet still
                           much better than on the stack where I found it.
                             At least this datastructure lives as much as the
                           whole schema change (I will change this in the
                           future)*/

    int sc_thd_failed;
    int schema_change;

    /*********************** temporary fields for table upgrade
     * ************************/
    unsigned long long start_genid;

    int already_finalized;

    int logical_livesc;
    int *sc_convert_done;
    unsigned int hitLastCnt;
    int got_tablelock;

    pthread_mutex_t livesc_mtx; /* mutex for logical redo */
    void *curLsn;

    /*********************** temporary fields for sbuf packing
     * ************************/
    /*********************** not needed for anything else
     * *****************************/

    size_t packed_len;

    unsigned views_locked : 1;
    unsigned is_osql : 1;
    unsigned set_running : 1;
    uint64_t seed;

    int (*publish)(tran_type *, struct schema_change_type *);
    void (*unpublish)(struct schema_change_type *);
};

typedef int (*ddl_t)(struct ireq *, struct schema_change_type *, tran_type *);

struct ireq;
typedef struct {
    tran_type *trans;
    struct ireq *iq;
    struct schema_change_type *sc;
    int started;
} sc_arg_t;

struct scinfo {
    int olddb_compress;
    int olddb_compress_blobs;
    int olddb_inplace_updates;
    int olddb_instant_sc;
    int olddb_odh;
};

enum schema_change_rc {
    SC_OK = 0,
    SC_COMMIT_PENDING,
    SC_CONVERSION_FAILED,
    SC_LLMETA_ERR,
    SC_PROPOSE_FAIL,
    SC_MASTER_DOWNGRADE,
    SC_ASYNC,
    SC_ASYNC_FAILED,
    SC_NOT_MASTER,
    SC_FAILED_TRANSACTION,
    SC_BDB_ERROR,
    SC_CSC2_ERROR,
    SC_INTERNAL_ERROR,
    SC_INVALID_OPTIONS,
    SC_TABLE_DOESNOT_EXIST,
    SC_TABLE_ALREADY_EXIST,
    SC_TRANSACTION_FAILED,
    SC_PAUSED,
    SC_ABORTED,
    SC_PREEMPTED,
    SC_DETACHED,
    SC_FUNC_IN_USE,
    SC_UNKNOWN_ERROR = -1,
    SC_CANT_SET_RUNNING = -99
};

enum schema_change_views_rc {
    SC_VIEW_NOERR = 0,
    SC_VIEW_ERR_GENERIC = -1,
    SC_VIEW_ERR_BUG = -2,
    SC_VIEW_ERR_EXIST = -3,
    SC_VIEW_ERR_SC = -4
};

enum schema_change_resume {
    SC_NOT_RESUME = 0,
    /* Non-transactional resume */
    SC_RESUME = 1,
    /* New master resuming transactional sc (without finalizing) */
    SC_NEW_MASTER_RESUME = 2,
    /* OSQL (block processor) picking up sc resumed by the new master */
    SC_OSQL_RESUME = 3,
    /* Resume a paused sc */
    SC_PREEMPT_RESUME = 4
};

enum schema_change_preempt {
    SC_ACTION_NONE = 0,
    SC_ACTION_PAUSE = 1,
    SC_ACTION_RESUME = 2,
    SC_ACTION_COMMIT = 3,
    SC_ACTION_ABORT = 4
};

typedef struct llog_scdone {
    void *handle;
    scdone_t type;
} llog_scdone_t;

size_t schemachange_packed_size(struct schema_change_type *s);
int start_schema_change_tran(struct ireq *, tran_type *tran);
int start_schema_change(struct schema_change_type *);
int finalize_schema_change(struct ireq *, tran_type *);
int create_queue(struct dbenv *, char *queuename, int avgitem, int pagesize);
int start_table_upgrade(struct dbenv *dbenv, const char *tbl,
                        unsigned long long genid, int full, int partial,
                        int sync);

/* Packs a schema_change_type struct into an opaque binary buffer so that it can
 * be stored in the low level meta table and the schema change can be resumed by
 * a different master if necessary.
 * Returns 0 if successful or <0 if failed.
 * packed is set to a pointer to the packed data and is owned by callee if this
 * function succeeds */
int pack_schema_change_type(struct schema_change_type *s, void **packed,
                            size_t *packed_len);

/* Unpacks an opaque buffer from the low level meta table into a
 * schema_change_type struct and the schema change can be resumed by a different
 * master if necessary.
 * Returns 0 if successful or <0 if failed.
 * If successfull, spoints to a pointer to a newly malloc'd schema_change_type
 * struct that has been populated with the unpacked values, the caller owns this
 * value.
 */
int unpack_schema_change_type(struct schema_change_type *s, void *packed,
                              size_t packed_len);

struct schema_change_type *
init_schemachange_type(struct schema_change_type *sc);

struct schema_change_type *new_schemachange_type();

void cleanup_strptr(char **schemabuf);

void free_schema_change_type(struct schema_change_type *s);

void *buf_put_schemachange(struct schema_change_type *s, void *p_buf,
                           void *p_buf_end);
void *buf_get_schemachange(struct schema_change_type *s, void *p_buf,
                           void *p_buf_end);
void *buf_get_schemachange_v1(struct schema_change_type *s, void *p_buf,
                              void *p_buf_end);
void *buf_get_schemachange_v2(struct schema_change_type *s, void *p_buf,
                              void *p_buf_end);
/* This belong into sc_util.h */
int check_sc_ok(struct schema_change_type *s);

int change_schema(char *table, char *fname, int odh, int compress,
                  int compress_blobs);

int live_sc_post_delete(struct ireq *iq, void *trans, unsigned long long genid,
                        const void *old_dta, unsigned long long del_keys,
                        blob_buffer_t *oldblobs);
int live_sc_post_update(struct ireq *iq, void *trans,
                        unsigned long long oldgenid, const void *old_dta,
                        unsigned long long newgenid, const void *new_dta,
                        unsigned long long ins_keys,
                        unsigned long long del_keys, int od_len, int *updCols,
                        blob_buffer_t *blobs, size_t maxblobs, int origflags,
                        int rrn, int deferredAdd, blob_buffer_t *oldblobs,
                        blob_buffer_t *newblobs);

int live_sc_post_add(struct ireq *iq, void *trans, unsigned long long genid,
                     uint8_t *od_dta, unsigned long long ins_keys,
                     blob_buffer_t *blobs, size_t maxblobs, int origflags,
                     int *rrn);

int live_sc_delayed_key_adds(struct ireq *iq, void *trans,
                             unsigned long long newgenid, const void *od_dta,
                             unsigned long long ins_keys, int od_len);

int live_sc_disable_inplace_blobs(struct ireq *iq);

int live_sc_delay_key_add(struct ireq *iq);

int add_schema_change_tables();

extern unsigned long long get_genid(bdb_state_type *, unsigned int dtastripe);
extern unsigned long long bdb_get_a_genid(bdb_state_type *bdb_state);

void handle_setcompr(SBUF2 *sb);

void vsb_printf(loglvl lvl, SBUF2 *sb, const char *sb_prefix,
                const char *prefix, const char *fmt, va_list args);
void sb_printf(SBUF2 *sb, const char *fmt, ...);
void sb_errf(SBUF2 *sb, const char *fmt, ...);

void sc_printf(struct schema_change_type *s, const char *fmt, ...);
void sc_errf(struct schema_change_type *s, const char *fmt, ...);
int do_dryrun(struct schema_change_type *);

extern int gbl_test_scindex_deadlock;

unsigned long long revalidate_new_indexes(struct ireq *iq, struct dbtable *db,
                                          uint8_t *new_dta,
                                          unsigned long long ins_keys,
                                          blob_buffer_t *blobs,
                                          size_t maxblobs);

char *get_ddl_type_str(struct schema_change_type *s);
char *get_ddl_csc2(struct schema_change_type *s);

int comdb2_is_user_op(char *user, char *password);

int llog_scdone_rename_wrapper(bdb_state_type *bdb_state,
                               struct schema_change_type *s, tran_type *tran,
                               int *bdberr);

#endif
