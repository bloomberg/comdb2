#include "comdb2.h"
#include "trigger.h"

int dbqueuedb_add_consumer(struct dbtable *db, int consumern, const char *method,
                           int noremove) {

    comdb2_queue_consumer_t *handler; 
    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler && handler->handles_method(method)) {
            return handler->add_consumer(db, consumern, method, noremove);
        }
    }
    return -1;
}

void dbqueuedb_admin(struct dbenv *dbenv) {
    comdb2_queue_consumer_t *handler; 
    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->admin(dbenv);
    }
}

int dbqueuedb_check_consumer(const char *method) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler && handler->handles_method(method))
            return handler->check_consumer(method);
    }
    return -1;
}

enum consumer_t dbqueue_consumer_type(struct consumer *c) {
    struct consumer_base b;
    memcpy(&b, c, sizeof(struct consumer_base));
    return b.type;
}

void dbqueuedb_coalesce(struct dbenv *dbenv) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->coalesce(dbenv);
    }
}

void dbqueuedb_restart_consumers(struct dbtable *db) {
    int type;
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->restart_consumers(db);
    }
}

void dbqueuedb_stop_consumers(struct dbtable *db) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->stop_consumers(db);
    }
}

void dbqueuedb_wake_all_consumers(struct dbtable *db, int force) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->wake_all_consumers(db, force);
    }
}

void dbqueuedb_wake_all_consumers_all_queues(struct dbenv *dbenv, int force) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->wake_all_consumers_all_queues(dbenv, force);
    }
}
