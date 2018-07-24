#if 0
consumer_type
dbqueue_add_consumer
dbqueue_admin
dbqueue_check_consumer
dbqueue_coalesce
dbqueue_restart_consumers
dbqueue_stop_consumers
dbqueue_wake_all_consumers
dbqueue_wake_all_consumers_all_queues
#endif
#include "comdb2.h"

int dbqueuedb_add_consumer(struct dbtable *db, int consumern, const char *method,
                         int noremove) {
    return 0;
}

void dbqueuedb_admin(struct dbenv *dbenv) {
}

int dbqueuedb_check_consumer(const char *method) {
    return 0;
}

enum consumer_t qconsumer_type(struct consumer *c) {
    return 0;
}

void dbqueuedb_coalesce(struct dbenv *dbenv) {
}

void dbqueuedb_restart_consumers(struct dbtable *db) {
}

void dbqueuedb_stop_consumers(struct dbtable *db) {
}

void dbqueue_wake_all_consumers(struct dbtable *db, int force);
void dbqueuedb_wake_all_consumers(struct dbtable *db, int force) {
    dbqueue_wake_all_consumers(db, force);
}

void dbqueuedb_wake_all_consumers_all_queues(struct dbenv *dbenv, int force) {
}
