#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h> // For va_list, va_start, va_end

#include <cdb2api.h>

pthread_mutex_t print_lock = PTHREAD_MUTEX_INITIALIZER;

const int gbl_num_threads = 10;
const char *gbl_dbname;
const char *gbl_tier;
const char *gbl_table_name;
int gbl_timeout_secs;

void concurrent_fprintf(FILE *stream, const char *fmt, ...) {
    va_list args;
    pthread_mutex_lock(&print_lock);
    fprintf(stream, "[PID %d | TID %p]: ", getpid(), (void *)pthread_self());
    va_start(args, fmt);
    vfprintf(stream, fmt, args);
    va_end(args);
    pthread_mutex_unlock(&print_lock);
}

// Open a connection to the database, run a query, and close the connection without draining the result set.
int waste_connection() {
    int rc = 0;
    cdb2_hndl_tp *db = NULL;
    concurrent_fprintf(stdout, "About to call cdb2_open\n");
    rc = cdb2_open(&db, gbl_dbname, gbl_tier, 0);
    if (rc) {
        concurrent_fprintf(stderr, "cdb2_open failed %d %s\n", rc, strerror(errno));
        goto err;
    }
    concurrent_fprintf(stdout, "Called cdb2_open\n");

    const char *sql_template = "select * from %s limit 1000";
    char sql[256];
    const int t_rc = snprintf(sql, sizeof(sql), sql_template, gbl_table_name);
    if (t_rc < 0 || t_rc >= sizeof(sql)) {
        concurrent_fprintf(stderr, "snprintf failed: %d %s\n", rc, strerror(errno));
        rc = t_rc;
        goto err;
    }
    concurrent_fprintf(stdout, "Called snprintf\n");
    cdb2_set_debug_trace(db);
    rc = cdb2_run_statement(db, sql);
    cdb2_unset_debug_trace(db);
    if (rc) {
        concurrent_fprintf(stderr, "cdb2_run_statement failed %d %s\n", rc, cdb2_errstr(db));
        goto err;
    }
    concurrent_fprintf(stdout, "Called cdb2_run_statement\n");
    rc = cdb2_next_record(db);
    if (rc != CDB2_OK) {
        concurrent_fprintf(stderr, "cdb2_next_record failed %d %s\n", rc, cdb2_errstr(db));
        goto err;
    }
    concurrent_fprintf(stdout, "Called cdb2_next_record\n");
    int val = *((int *)cdb2_column_value(db, 0));
    if (val != 1) {
        concurrent_fprintf(stderr, "Expected value 1, got %d\n", val);
        rc = -1;
        goto err;
    }
    concurrent_fprintf(stdout, "Called cdb2_column_value\n");
err:
    cdb2_close(db);
    return rc;
}


// This thread will open a connection to the database, run a query, and close the connection without draining
// the result set. This makes it a 'connection waster' because this undrained connection cannot be reused.
// It will repeat this process indefinitely. This kind of workload has exhausted file descriptors in the past
// because connections were opened faster than the database could close them.
void *connection_waster_thd(void *pp) {
    int rc = 0;
    setenv("SSL_MODE", "ALLOW", 1);
    const int time_to_sleep_between_connections = 500000; // 500 milliseconds
    const time_t start_time = time(NULL);
    do {
        concurrent_fprintf(stdout, "starting a connection cycle\n");
        rc = waste_connection();
        usleep(time_to_sleep_between_connections);
        concurrent_fprintf(stdout, "completed a connection cycle\n");
    } while (!rc && ((time(NULL) - start_time) < gbl_timeout_secs));
    return (void *)(intptr_t)rc;
}

int run_test() {
    int rc = 0;

    // Create threads that waste connections
    pthread_t tid[gbl_num_threads];
    for (int i = 0; i < gbl_num_threads; i++) {
        rc = pthread_create(&tid[i], NULL, connection_waster_thd, NULL);
        if (rc) {
            concurrent_fprintf(stderr, "pthread_create failed %d %s\n", rc, strerror(rc));
            goto err;
        }
    }

    // Wait for all connection waster threads to finish
    for (int i = 0; i < gbl_num_threads; i++) {
        void * thread_rc;
        const int t_rc = pthread_join(tid[i], (void **) &thread_rc);
        if (t_rc) {
            concurrent_fprintf(stderr, "pthread_join failed %d %s\n", t_rc, strerror(t_rc));
            goto err;
        } else if (thread_rc != 0) {
            concurrent_fprintf(stderr, "%d: Thread %lu failed with return code %d\n", getpid(), tid[i], (int)(intptr_t)thread_rc);
            rc = 1;
            // Don't exit immediately, wait for all threads to finish
        }
    }

err:
    return rc;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        concurrent_fprintf(stderr, "Usage: %s <dbname> <tier> <table_name> <timeout>\n", argv[0]);
        return 1;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    signal(SIGPIPE, SIG_IGN);

    const char *conf = getenv("CDB2_CONFIG");
    if (conf) { cdb2_set_comdb2db_config(conf); }

    gbl_dbname = argv[1];
    gbl_tier = argv[2];
    gbl_table_name = argv[3];
    gbl_timeout_secs = atoi(argv[4]);
    return run_test();
}
