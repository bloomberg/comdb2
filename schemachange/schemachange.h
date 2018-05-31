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

/* status for schema_change_type->addonly */
enum { SC_NOT_ADD = 0, SC_TO_ADD = 1, SC_DONE_ADD = 2 };

struct schema_change_type {
    /*  ==========    persistent members ========== */
    unsigned long long rqid;
    uuid_t uuid;
    int type; /* DBTYPE_TAGGED_TABLE or DBTYPE_QUEUE or DBTYPE_QUEUEDB
                 or DBTYPE_MORESTRIPE */
    size_t table_len;
    char table[MAXTABLELEN]; /* name of table/queue */
    int rename;              /* new table name */
    char newtable[MAXTABLELEN]; /* rename table */
    size_t fname_len;
    char fname[256];         /* name of schema file for table schema change
                                or client provided SP version */
    size_t aname_len;
    char aname[256];         /* advised file name for .csc2 */
    int avgitemsz;           /* average item size for queue creation */
    int fastinit;            /* are we doing "fast init?" */
    int newdtastripe;        /* new dtastripe factor */
    int blobstripe;          /* 1 if we are converting to blobstripe */
    int live;
    int addonly;
    int partialuprecs; /* 1 if we're doing partial-table upgrade */
    int fulluprecs;    /* 1 if we're doing full-table upgrade */
    int alteronly;
    int is_trigger;
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
    int ip_updates;     /* inplace updates or -1 for no change */
    int instant_sc;     /* 1 is enable, 0 disable, or -1 for no change */
    int doom;
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
#define SC_MASK_FLG                                                            \
    0xfffffffcU /* Detect and fail if newer ver started sc.                    \
                   Update this mask when new flags added */
    uint32_t flg;

    uint8_t rebuild_index;    /* option to rebuild only one index */
    uint8_t index_to_rebuild; /* can use just a short for rebuildindex */

    char original_master_node[256];
    int drop_table;

    LISTC_T(struct dest) dests;

    size_t spname_len;
    char spname[MAX_SPNAME];
    int addsp;
    int delsp;
    int defaultsp;
    int is_sfunc; /* lua scalar func */
    int is_afunc; /* lua agg func */

    /* ========== runtime members ========== */
    int onstack; /* if 1 don't free */
    int nothrevent;
    int pagesize; /* pagesize override to use */
    int showsp;
    SBUF2 *sb; /* socket to sponsoring program */
    int must_close_sb;
    int use_old_blobs_on_rebuild;

    int resume;           /* if we are trying to resume a schema change,
                           * usually because there is a new master */
    int retry_bad_genids; /* retrying a schema change (with full rebuild)
                             because there are old genids in flight */
    int dryrun;           /* comdb2sc.tsk -y */
    int statistics;       /* comdb2sc.tsk <dbname> stat <table> */
    int use_new_genids;   /* rebuilding old genids needs to
                             get new genids to avoid name collission */
    int finalize;      /* Whether the schema change should be committed */
    int finalize_only; /* only commit the schema change */

    pthread_mutex_t mtx; /* mutex for thread sync */
    int sc_rc;

    struct ireq *iq;
    void *tran; /* transactional schemachange */

    struct schema_change_type *sc_next;

    int usedbtablevers;

    /*********************** temporary fields for in progress
     * schemachange************/
    /********************** it will change eventually (do not try to serialize)
     * ************/

    struct dbtable *db;
    struct dbtable *newdb;
    struct scplan plan; /**** TODO This is an abomination, i know. Yet still
                           much better than on the stack where I found it.
                             At least this datastructure lives as much as the
                           whole schema change (I will change this in the
                           future)*/

    /*********************** temporary fields for table upgrade
     * ************************/
    unsigned long long start_genid;

    int already_finalized;

    /*********************** temporary fields for sbuf packing
     * ************************/
    /*********************** not needed for anything else
     * *****************************/

    size_t packed_len;
};

struct ireq;
typedef struct {
    tran_type *trans;
    struct ireq *iq;
    struct schema_change_type *sc;
} sc_arg_t;

struct scinfo {
    int olddb_compress;
    int olddb_compress_blobs;
    int olddb_inplace_updates;
    int olddb_instant_sc;
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
    SC_RESUME = 1,
    SC_NEW_MASTER_RESUME = 2
};

#include <bdb_schemachange.h>
typedef struct llog_scdone {
    void *handle;
    scdone_t type;
} llog_scdone_t;

size_t schemachange_packed_size(struct schema_change_type *s);
int start_schema_change_tran(struct ireq *, tran_type *tran);
int start_schema_change(struct schema_change_type *);
int finalize_schema_change(struct ireq *, tran_type *);
int create_queue(struct dbenv *, char *queuename, int avgitem, int pagesize,
                 int isqueuedb);
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

#endif
