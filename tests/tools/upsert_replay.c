#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>

#include <cdb2api.h>

char *dbname;
char *tier;
int nops;

void *thd(void *unused) {
    int rc;
    cdb2_hndl_tp *db;
    rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "open %d %s\n", rc, cdb2_errstr(db));
        return NULL;
    }
    for(int i = 0; i < nops; i++) {
        int id;
        id = rand() % 400;
        cdb2_clearbindings(db);
        cdb2_bind_param(db, "a", CDB2_INTEGER, &id, sizeof(int));
        rc = cdb2_run_statement(db, "insert into t values(@a, now()) on conflict(a) do update set b=now()");
        if (rc) {
            fprintf(stderr, "run %d %s\n", rc, cdb2_errstr(db));
            return NULL;
        }
        while ((rc = cdb2_next_record(db)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "next %d %s\n", rc, cdb2_errstr(db));
            return NULL;
        }
    }
    cdb2_close(db);
    return NULL;
}

void* truncate_thd(void *unused) {
    int rc;
    cdb2_hndl_tp *db;
    rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "open %d %s\n", rc, cdb2_errstr(db));
        return NULL;
    }
    for (;;) {
        rc = cdb2_run_statement(db, "truncate t");
        if (rc) {
            fprintf(stderr, "truncate %d %s\n", rc, cdb2_errstr(db));
            return NULL;
        }
        sleep(5);
    }
}

int main(int argc, char *argv[]) {
    int nthreads = 2;
    pthread_t tid[nthreads];

    if (argc != 4) {
        fprintf(stderr, "usage: dbname tier count\n");
        exit(1);
    }
    dbname = argv[1];
    tier = argv[2];
    nops = atoi(argv[3]);

    int rc;
    cdb2_hndl_tp *db;
    rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "open %d %s\n", rc, cdb2_errstr(db));
        exit(1);
    }
    cdb2_run_statement(db, "truncate t");

    for (int i = 0; i < nthreads; i++) {
        int rc = pthread_create(&tid[i], NULL, thd, NULL);
        if (rc) {
            fprintf(stderr, "pthread_create rc %d %s\n", rc, strerror(rc));
            return 1;
        }
    }

    pthread_t truncator;
    pthread_create(&truncator, NULL, truncate_thd, NULL);

    for (int i = 0; i < nthreads; i++) {
        void *st;
        int rc = pthread_join(tid[i], &st);
        if (rc) {
            fprintf(stderr, "pthread_join rc %d %s\n", rc, strerror(rc));
            return 1;
        }
    }
}
