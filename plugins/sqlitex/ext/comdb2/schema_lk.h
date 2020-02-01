#ifndef INCLUDE_SCHEMA_LK
#define INCLUDE_SCHEMA_LK

#include <pthread.h>
extern pthread_rwlock_t schema_lk;

#define rdlock_schema_lk() pthread_rwlock_rdlock(&schema_lk)
#define unlock_schema_lk() pthread_rwlock_unlock(&schema_lk)
#endif
