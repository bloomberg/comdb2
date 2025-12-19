#include <signal.h>
#include <stdio.h>
#include <cdb2api.h>
#include <cdb2api_test.h>
#include <sys/time.h>

static int64_t epochms()
{
    struct timeval now;
    if (gettimeofday(&now, NULL) != 0)
        return 0;
    return (now.tv_sec * 1000 + now.tv_usec / 1000);
}

int test_enforce_timeout_open_sockpool(const char *dbname, const char *type)
{
    cdb2_enable_sockpool();
    int rc = 0;
    cdb2_hndl_tp *cdb2h = NULL;
    int64_t start, end;
    start = epochms();
    rc = cdb2_open(&cdb2h, "proddb", "prod", 0);
    if (rc != CDB2_OK) {
        end = epochms();
        fprintf(stderr, "Can't open db:%s cluster:%s within:%ld %s\n", dbname, type, end-start, cdb2_errstr(cdb2h));
        if (end - start > 5100) {
            cdb2_close(cdb2h);
            return -1;
        }
        cdb2_close(cdb2h);
        return 0;
    }
    return -1;
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *db = argv[1];
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    int rc = test_enforce_timeout_open_sockpool(db, "default");
    
    return rc;
}
