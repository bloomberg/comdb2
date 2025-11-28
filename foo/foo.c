#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include <cdb2api.h>

static char *dbname = "akdb";
static char *tier = "default";
static int flags;
static int conns = 10;

static void *work(void *data)
{
    cdb2_hndl_tp *dbs[conns];
    int rc, bad_open = 0;
    for (int i = 0; i < conns; ++i) {
        while ((rc = cdb2_open(&dbs[i], dbname, tier, flags)) != 0) {
            ++bad_open;
        }
    }
    if (bad_open) {
        printf("cdb2_open retries:%d", bad_open);
    }
    while (1) {
        for (int i = 0; i < conns; ++i) {
            cdb2_hndl_tp *db = dbs[i];
            rc = cdb2_run_statement(db, "select value from generate_series(1, 100)");
            if (rc != 0) {
                continue;
            }
            while ((rc = cdb2_next_record(db)) == CDB2_OK)
                ;
            if (rc != CDB2_OK_DONE) {
                fprintf(stderr, "cdb2_next_record rc:%d err:%s\n", rc, cdb2_errstr(db));
            }
        }
    }
    return NULL;
}
int main(int argc, char **argv)
{
    int sigignore(int);
    sigignore(SIGPIPE);
    int thds;
    switch (argc) {
    case 5: tier = argv[4];
    case 4: dbname = argv[3];
    case 3: conns = atoi(argv[2]);
    case 2: thds = atoi(argv[1]);
        break;
    default:
        fprintf(stderr, "usage: foo <thds> <conns-per-thd> [dbname] [tier]\n");
        exit(1);
    }
    printf("thds:%d  conns-per-thd:%d  dbname:%s  tier:%s\n", thds, conns, dbname, tier);
    if (strcmp(tier, "default") != 0) {
        flags |= CDB2_DIRECT_CPU;
    }
    pthread_t t[thds - 1];
    for (int i = 0; i < thds - 1; ++i) {
        if (pthread_create(&t[i], NULL, work, NULL) != 0) {
            perror("pthread_create");
            exit(2);
        }
    }
    work("main");
    void *ret;
    for (int i = 0; i < thds - 1; ++i) {
        pthread_join(t[i], &ret);
    }
}
