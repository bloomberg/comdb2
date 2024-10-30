#ifndef LOG_QUEUE_TRIGGER_H
#define LOG_QUEUE_TRIGGER_H
#include <bdb_int.h>

enum { LOGQTRIGGER_PUSH = 1, LOGQTRIGGER_PULL = 2, LOGQTRIGGER_MASTER_ONLY = 4 };

typedef void (*logqueue_trigger_callback)(bdb_state_type *bdb_state, const DB_LSN *commit_lsn, const char *filename,
                                          const DBT *key, const DBT *data, void *userptr);

/* Log-trigger invokes callback when a record is written to a queue in push-mode */
void *register_logqueue_trigger(const char *filename, bdb_state_type *(*gethndl)(const char *q),
                                logqueue_trigger_callback func, void *userptr, size_t maxsz, uint32_t flags);

/* Pull mode removes record from front of queue */
int logqueue_trigger_get(void *qfile, DBT *key, DBT *data, int timeoutms);

/* Test function */
void register_dump_qtrigger(const char *filename, bdb_state_type *(*gethndl)(const char *q), const char *outfile,
                            int maxsz);

#ifdef WITH_QKAFKA

int register_queue_kafka(const char *filename, const char *kafka_topic, bdb_state_type *(*gethndl)(const char *q),
                         int maxsz);

#endif

#endif
