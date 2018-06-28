#ifndef __bdb_osql_log_rec_h
#define __bdb_osql_log_rec_h

/**
 * Contains a copy of one log record
 *
 */
typedef struct bdb_osql_log_rec {
    int type; /* type of the record */
    DB_LSN lsn;
    unsigned long long genid;
    char *table;
    int dbnum;
    short dtafile;
    short dtastripe;
    DB_LSN complsn;
    struct bdb_osql_log_rec *comprec;
    LINKC_T(struct bdb_osql_log_rec) lnk; /* link to next record */

} bdb_osql_log_rec_t;

typedef struct bdb_osql_log_impl {
    LISTC_T(bdb_osql_log_rec_t) recs; /* list of undo records */
    int clients;                      /* clean ; num sessions need it */
    int trak;                         /* set this for debug tracing */
    unsigned long long oldest_genid;  /* oldest genid in this log */
    unsigned long long commit_genid;  /* The commit id of log.*/
    int cancelled; /* if clients>0, set this if already cancelled */
} bdb_osql_log_impl_t;

#endif
