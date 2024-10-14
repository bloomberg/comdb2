#ifndef LOG_TRIGGER_H
#define LOG_TRIGGER_H

#include <build/db.h>

#define LOG_TRIGGER_VERBOSE 1

int log_trigger_add_table(char *tablename);

void *log_trigger_btree_trigger(const char *btree);

int log_trigger_callback(void *, const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename, u_int32_t rectype,
                         const void *log);

int log_trigger_register(const char *tablename,
                         int (*cb)(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename, u_int32_t rectype,
                                   const void *log),
                         u_int32_t flags);

#endif
