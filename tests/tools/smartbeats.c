#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include <cdb2api.h>

const char *db, *tier;
int nheartbeats;
int laststate = 1;

/* Check a heartbeat. If no progress has been made within 2 seconds, abort. */
static void *processing_heartbeat(cdb2_hndl_tp *hndl, void *user_arg,
                                  int argc, void **argv)
{
    if (argc != 1)
        return NULL;

    int state = (intptr_t)argv[0];

    if (!state && !laststate) {
        puts("Not making progress in 2 seconds!");
        abort();
    }

    printf("Received non-zero state in %p.\n", (void *)pthread_self());

    laststate = state;
    ++nheartbeats;

    return NULL;
}

void *consumer(void *_)
{
    cdb2_hndl_tp *h;
    cdb2_open(&h, db, tier, 0);
    cdb2_event *e = cdb2_register_event(h, CDB2_AT_RECEIVE_HEARTBEAT, 0,
                                        processing_heartbeat, NULL,
                                        1, CDB2_QUERY_STATE);
    cdb2_run_statement(h, "exec procedure w()");
    cdb2_unregister_event(h, e);
    cdb2_close(h);
    return NULL;
}

static int TEST_heartbeat_events()
{
    int rc;
    cdb2_hndl_tp *h;
    cdb2_event *e;
    pthread_t t1, t2;


    cdb2_open(&h, db, tier, 0);
    e = cdb2_register_event(h, CDB2_AT_RECEIVE_HEARTBEAT, 0,
                            processing_heartbeat, NULL, 1, CDB2_QUERY_STATE);

    /*************************************************
     *           Testcase 1: SELECT SLEEP(N)         *
     *************************************************/

    rc = cdb2_run_statement(h, "SELECT SLEEP(10)");
    if (rc != 0)
        return rc;
    while ((rc = cdb2_next_record(h)) == CDB2_OK);

    /*************************************************
     *           Testcase 2: Consumers               *
     *************************************************/

    rc = cdb2_run_statement(h, "create table c (i int)");
    if (rc != 0)
        return rc;
    rc = cdb2_run_statement(h, "create procedure w { local function main (e) local c = db:consumer() local e = c:get() end }");
    if (rc != 0)
        return rc;
    rc = cdb2_run_statement(h, "create lua consumer w on (table c for insert)");
    if (rc != 0)
        return rc;

    /*************************************************
     *   Testcase 2A: A consumer with no events      *
     *************************************************/
    pthread_create(&t1, NULL, consumer, NULL);

    /*************************************************
     *   Testcase 2B: A consumer registering         *
     *                itself with master             *
     *************************************************/
    pthread_create(&t2, NULL, consumer, NULL);

    sleep(10);

    /*************************************************
     *      Testcase 3: comdb2_transaction_logs      *
     *************************************************/

    cdb2_run_statement(h, "set maxquerytime 10");
    cdb2_run_statement(h, "select * from comdb2_transaction_logs where flags = 1");
    while (cdb2_next_record(h) == CDB2_OK);

    cdb2_unregister_event(h, e);
    cdb2_close(h);

    /* Make sure heartbeats were indeed checked. */
    return (nheartbeats == 0);
}

int main(int argc, char **argv)
{
    char *conf = getenv("CDB2_CONFIG");
    tier = "local";
    db = argv[1];

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = "default";
    }

    if (argc >= 3)
        tier = argv[2];

    return TEST_heartbeat_events();
}
