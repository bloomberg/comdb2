#include <pthread.h>

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <time.h>

#include <cdb2api.h>

int verify_errors = 0;
int zero_updates = 0;
int updates = 0;
int nthreads = 1;
int nruns = 0;
char *dbname;
char *tier;
char *table;
int id = 0;
int total_runs;

pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
int qid = 0;

pthread_t tids[1000];

void *thd(void *unused) {
    cdb2_hndl_tp *db;
    cdb2_effects_tp eff = {0};
    int rc;
    int nruns = total_runs;

    printf("nruns %d\n", nruns);

    rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "open %d %s\n", rc, cdb2_errstr(db));
        exit(1);
    }

    rc = cdb2_run_statement(db, "set verifyretry off");
    if (rc) {
        fprintf(stderr, "set %d %s\n", rc, cdb2_errstr(db));
        exit(1);
    }

    int id;
    while (nruns > 0) {
        pthread_mutex_lock(&lk);
        id = qid++;
        pthread_mutex_unlock(&lk);

        char sql[1024];

        sprintf(sql, "update %s set total_count=total_count+1 where id=74 /* %d */", table, id);
        rc = cdb2_run_statement(db, sql);
        if (rc == CDB2ERR_VERIFY_ERROR) {
            verify_errors++;
            continue;
        }
        else if (rc) {
            fprintf(stderr, "update %d %s\n", rc, cdb2_errstr(db));
            exit(1);
        }

        rc = cdb2_get_effects(db, &eff);
        if (rc) {
            fprintf(stderr, "get_effects %d %s\n", rc, cdb2_errstr(db));
            exit(1);
        }
        if (eff.num_updated == 0) {
            zero_updates++;
            printf("zero updates for %d\n", id);
            fflush(NULL);
            continue;
        }
        updates++;
        nruns--;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    int rc;

    if (argc != 6) {
        fprintf(stderr, "usage: dbname tier tablename nruns nthreads\n");
        exit(1);
    }
	char *conf = getenv("CDB2_CONFIG");
	if (conf)
		cdb2_set_comdb2db_config(conf);
    dbname = argv[1];
    tier = argv[2];
    table = argv[3];
    total_runs = atoi(argv[4]);
    nthreads = atoi(argv[5]);

    printf("nthread %d\n", nthreads);

    for (int i = 0 ; i < nthreads; i++) {
        pthread_create(&tids[i], NULL, thd, NULL);
    }
    for (int i = 0 ; i < nthreads; i++) {
        void *p;
        pthread_join(tids[i], &p);
    }
    return !!zero_updates;
}
