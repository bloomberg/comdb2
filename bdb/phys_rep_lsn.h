#ifndef PHYS_REP_LSN_H
#define PHYS_REP_LSN_H

#include <stdint.h>
#include <time.h>

struct __db_env;
struct bdb_state_tag;
struct cdb2_hndl;

#include <log_info.h>

typedef unsigned char u_int8_t;

/* Mark this table to be ignored (not replicated to) */
int physrep_add_ignore_table(char *tablename);

/* Return 1 if this btree should be ignored */
int physrep_ignore_btree(const char *filename);

/* Return 1 if this table should be ignored */
int physrep_ignore_table(const char *tablename);

/* Return count of ignored tables */
int physrep_ignore_table_count(void);

/* List ignored tables */
int physrep_list_ignored_tables(void);

int apply_log(struct bdb_state_tag *, unsigned int file, unsigned int offset, int64_t rectype, void *blob,
              int blob_len);
int truncate_log_lock(struct bdb_state_tag *, unsigned int file,
                      unsigned int offset, uint32_t flags);
int find_log_timestamp(struct bdb_state_tag *, time_t time, unsigned int *file,
                       unsigned int *offset);

/* Best-effort probe: does the connected source cover our current log range?
 * Returns 1 (covers), 0 (no overlap), or -1 (unknown/transient). */
int physrep_source_covers_me(void *bdb_state, struct cdb2_hndl *repl_db);

LOG_INFO find_match_lsn(void *bdb_state, struct cdb2_hndl *repl_db, LOG_INFO start_info);
int physrep_find_last_match(void *bdb_state, struct cdb2_hndl *repl_db, LOG_INFO match_info, LOG_INFO my_end,
                            LOG_INFO *last_match);
int physrep_logged_gen_ahead(void *bdb_state);

#endif
