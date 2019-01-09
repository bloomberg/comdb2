#ifndef PHYS_REP_LSN_H
#define PHYS_REP_LSN_H

#include <stdint.h>
#include <time.h>

typedef struct LOG_INFO LOG_INFO;
struct LOG_INFO {
    uint32_t file;
    uint32_t offset;
    uint32_t size;
    uint32_t gen;
};

struct __db_env;
struct bdb_state_tag;

LOG_INFO get_last_lsn(struct bdb_state_tag *);
uint32_t get_next_offset(struct __db_env *, LOG_INFO log_info);
int apply_log(struct __db_env *, unsigned int file, unsigned int offset,
              int64_t rectype, void *blob, int blob_len);
int truncate_log_lock(struct bdb_state_tag *, unsigned int file,
                      unsigned int offset, uint32_t flags);
int find_log_timestamp(struct bdb_state_tag *, time_t time, unsigned int *file,
                       unsigned int *offset);

#endif
