#include "comdb2.h"
#include "trigger.h"

#include <unistd.h>
#include <poll.h>
#include <locks.h>

extern int gbl_debug_systable_locks;
extern int gbl_assert_systable_locks;
/* Wrapper for queue consumers. Core db code calls these, which then find
 * the first consumer plugin that knows how to handle the request, and dispatch
 * to it. */

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

/* This gets called for all plugins, not just the first */
void dbqueuedb_admin(struct dbenv *dbenv) {
    comdb2_queue_consumer_t *handler; 
    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler)
            handler->admin(dbenv, i);
    }
}

int dbqueuedb_check_consumer(const char *method) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler && handler->handles_method(method)) {
            return handler->check_consumer(method);
        }
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

int dbqueuedb_restart_consumers(struct dbtable *db) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler) {
            int rc = handler->restart_consumers(db);
            if (rc == 0)
                return 0;
        }
    }
    return -1;
}

int dbqueuedb_stop_consumers(struct dbtable *db) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler) {
            int rc = handler->stop_consumers(db);
            if (rc == 0)
                return 0;
        }
    }
    return -1;
}

int dbqueuedb_wake_all_consumers(struct dbtable *db, int force) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler) {
            int rc = handler->wake_all_consumers(db, force);
            if (rc == 0)
                return 0;
        }
    }
    return -1;
}

int dbqueuedb_wake_all_consumers_all_queues(struct dbenv *dbenv, int force) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler) {
            int rc = handler->wake_all_consumers_all_queues(dbenv, force);
            if (rc == 0)
                return 0;
        }
    }
    return -1;
}

int dbqueuedb_get_name(struct dbtable *db, char **spname) {
    comdb2_queue_consumer_t *handler; 

    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler) {
            int rc = handler->get_name(db, spname);
            if (rc == 0)
                return 0;
        }
    }
    return -1;
}

static int dbqueuedb_get_stats_int(struct dbtable *db, tran_type *tran,
                                   struct consumer_stat *stats)
{
    comdb2_queue_consumer_t *handler;
    for (int i = 0; i < CONSUMER_TYPE_LAST; i++) {
        handler = thedb->queue_consumer_handlers[i];
        if (handler) {
            int rc = handler->get_stats(db, 0, tran, stats);
            if (rc == 0)
                return 0;
        }
    }
    return -1;
}

int dbqueuedb_get_stats(struct dbtable *db, struct consumer_stat *stats, uint32_t lockid)
{
    int rc;
    int bdberr;
    uint32_t savedlid;
    bdb_state_type *bdb_state = db->handle;
    tran_type *trans = bdb_tran_begin(bdb_state, NULL, &bdberr);
    if (!trans) {
        logmsg(LOGMSG_ERROR, "%s bdb_tran_begin:%s bdberr:%d\n", __func__,
               db->tablename, bdberr);
        return -1;
    }
    if (lockid) {
        bdb_get_tran_lockerid(trans, &savedlid);
        bdb_set_tran_lockerid(trans, lockid);

        if (gbl_debug_systable_locks) {
            bdb_assert_tablename_locked(bdb_state, "_comdb2_systables", lockid, ASSERT_TABLENAME_LOCKED_READ);
        }

        if (gbl_assert_systable_locks) {
            bdb_assert_tablename_locked(bdb_state, "comdb2_queues", lockid, ASSERT_TABLENAME_LOCKED_READ);
        }
    }
    if ((rc = bdb_lock_table_read(bdb_state, trans)) == 0) {
        rc = dbqueuedb_get_stats_int(db, trans, stats);
    } else {
        logmsg(LOGMSG_ERROR, "%s bdb_lock_table_read:%s rc:%d\n", __func__,
               db->tablename, rc);
    }
    if (lockid) {
        bdb_set_tran_lockerid(trans, savedlid);
    }
    bdb_tran_abort(bdb_state, trans, &bdberr);
    return rc;
}

int queue_consume(struct ireq* iq, const void* fnd, int consumern)
{
    const int sleeptime = 1;
    int gotlk = 0;

    /* Outer loop - long sleep between retries */
    while(1)
    {
        int retries;

        /* Inner loop - short delay between retries */
        for(retries = 0; retries < gbl_maxretries; retries++)
        {
            tran_type *trans;
            int rc;

            if(retries > 10)
                poll(0,0,(rand()%25+1));

            if (gbl_exclusive_blockop_qconsume) {
                Pthread_rwlock_wrlock(&gbl_block_qconsume_lock);
                gotlk = 1;
            }

            rc = trans_start(iq, NULL, &trans);
            if(rc != 0)
            {
                if (gotlk)
                    Pthread_rwlock_unlock(&gbl_block_qconsume_lock);
                return -1;
            }

            if (gbl_debug_systable_locks) {
                rc = bdb_lock_tablename_read(thedb->bdb_env, "_comdb2_systables", trans);
            }

            if (rc == 0) {
                rc = dbq_consume(iq, trans, consumern, fnd);
            } else {
                rc = RC_INTERNAL_RETRY;
            }
            if(rc != 0)
            {
                trans_abort(iq, trans);
                if (gotlk)
                    Pthread_rwlock_unlock(&gbl_block_qconsume_lock);
                if(rc == RC_INTERNAL_RETRY)
                    continue;
                else if(rc == IX_NOTFND)
                    return 0;
                else
                    break;
            }

            rc = trans_commit(iq, trans, 0);
            if (gotlk)
                Pthread_rwlock_unlock(&gbl_block_qconsume_lock);
            if(rc == 0)
                return 0;
            else if(rc == RC_INTERNAL_RETRY)
                continue;
            else if(rc == ERR_NOMASTER)
                return -1;
            else
                break;
        }

        logmsg(LOGMSG_ERROR, "difficulty consuming key from queue '%s' consumer %d\n",
                iq->usedb->tablename, consumern);
        if (db_is_stopped() || thedb->master != gbl_myhostname)
            return -1;
        sleep(sleeptime);
    }
}
