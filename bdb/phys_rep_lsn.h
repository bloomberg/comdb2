#ifndef PHYS_REP_LSN_H
#define PHYS_REP_LSN_H

#include <build/db.h>
#include "bdb_int.h"
#include <time.h>

typedef struct LOG_INFO LOG_INFO;
struct LOG_INFO {
    u_int32_t file;
    u_int32_t offset;
    u_int32_t size;
    u_int32_t gen;
};

LOG_INFO get_last_lsn(bdb_state_type *bdb_state);
u_int32_t get_next_offset(DB_ENV *dbenv, LOG_INFO log_info);

int apply_log(DB_ENV *dbenv, unsigned int file, unsigned int offset,
              int64_t rectype, void *blob, int blob_len);
int truncate_log_lock(bdb_state_type *bdb_state, unsigned int file,
                      unsigned int offset, uint32_t flags);
int compare_log(bdb_state_type *bdb_state, unsigned int file,
                unsigned int offset, void *blob, unsigned int blob_len);
int find_log_timestamp(bdb_state_type *bdb_state, time_t time,
                       unsigned int *file, unsigned int *offset);

int get_next_matchable(LOG_INFO *info);
int open_db_cursor(bdb_state_type *bdb_state);
void close_db_cursor();

#endif
