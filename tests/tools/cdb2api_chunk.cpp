#include <cstdio>
#include <cstdlib>
#include <cdb2api.h>

static cdb2_hndl_tp *hndl;
static int num_heartbeats = 0;

static int exit_failure(void)
{
    fprintf(stderr, "num_heartbeats:%d\n", num_heartbeats);
    fprintf(stderr, "%s\n", cdb2_errstr(hndl));
    return EXIT_FAILURE;
}

static void *on_heartbeat(cdb2_hndl_tp *db, void *dummy0, int dummy1, void **dummy2)
{
    ++num_heartbeats;
    return NULL;
}

int main(int argc, char **argv)
{
    char *dbname = argv[1];
    char *tier = argv[2];
    int total = atoi(argv[3]);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if (cdb2_open(&hndl, argv[1], argv[2], 0) != 0) return exit_failure();
    if (cdb2_register_event(hndl, CDB2_AT_RECEIVE_HEARTBEAT, (cdb2_event_ctrl)0, on_heartbeat, NULL, 0) == NULL) return exit_failure();
    if (cdb2_run_statement(hndl, "drop table if exists chunk") != 0) return exit_failure();
    if (cdb2_run_statement(hndl, "create table chunk(i longlong primary key, hello cstring(64) default('hello, world!') index, d datetime default(now()) index)") != 0) return exit_failure();
    if (cdb2_run_statement(hndl, "set transaction chunk 50") != 0) return exit_failure();
    if (cdb2_run_statement(hndl, "begin") != 0) return exit_failure();
    int i;
    cdb2_bind_param(hndl, "i", CDB2_INTEGER, &i, sizeof(i));
    for (i = 0; i < total; ++i) {
        if (cdb2_run_statement(hndl, "insert into chunk(i) values(@i)") != 0) return exit_failure();
    }
    if (cdb2_run_statement(hndl, "commit") != 0) return exit_failure();
    if (cdb2_clearbindings(hndl) != 0) return exit_failure();
    if (cdb2_close(hndl) != 0) return exit_failure();
    fprintf(stdout, "num_heartbeats:%d\n", num_heartbeats);
    return EXIT_SUCCESS;
}
