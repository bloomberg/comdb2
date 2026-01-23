#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cdb2api.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

pthread_t fork_tid, request_tid;
static char *dbname = NULL;
static char *tier = NULL;

void* request_thd(void *ununsed) {
    cdb2_hndl_tp *db;
    for (int i=0;i<10000;i++) {
        int rc = cdb2_open(&db, dbname, tier, 0);
        if (rc) {
            fprintf(stderr, "cdb2_open failed %s\n", cdb2_errstr(db));
            abort();
	}
        rc = cdb2_run_statement(db, "select 1");
        if (rc) {
            fprintf(stderr, "cdb2_run_statement failed %s\n", cdb2_errstr(db));
            abort();
	}
        while ((rc = cdb2_next_record(db)) == CDB2_OK);
        if (rc != CDB2_OK_DONE) {
            abort();
	}
        cdb2_close(db);
    }
    return NULL;
}

void* fork_thd(void *p) {
    for (int i=0;i<10000;i++) {
        pid_t pid = fork();
        if (pid == -1)
            abort();
        else if (pid == 0) {
            int rc = execl("/bin/echo", "echo", "echo", (char*)NULL);
            if (rc != 0) {
                perror("execl failed");
                abort();
	    }
            // unreachable?
            exit(1);
        }
        else {
            pid_t wp;
            int status;
            wp = waitpid(pid, &status, 0);
            if (wp != pid)
                abort();
            if (status != 0)
                abort();
        }
    }
    return NULL;
}


void forker(void) {
    int rc = pthread_create(&fork_tid, NULL, fork_thd, NULL);
    if (rc)
        abort();
}

void requester(void) {
    int rc = pthread_create(&request_tid, NULL, request_thd, NULL);
    if (rc)
        abort();
}

void waitforthd(void) {
    void *ret;
    int rc = pthread_join(fork_tid, &ret);
    if (rc)
        abort();
    rc = pthread_join(request_tid, &ret);
    if (rc)
        abort();
}

int main(int argc, char **argv)
{
    if (argc < 1) {
        fprintf(stderr, "Usage: %s <dbname> \n", argv[0]);
        return 1;
    }

    char *conf = getenv("CDB2_CONFIG");

    if (conf != NULL) {
        cdb2_set_comdb2db_config(conf);
        tier = (char*)"default";
    } else {
        tier = (char*)"local";
    }

    dbname = argv[1];

    signal(SIGPIPE, SIG_IGN);
    forker();
    requester();
    waitforthd();
    return 0;
}
