/*
 * trace_test - run SQL with a comdb2_trace payload on every statement
 *
 * Usage: trace_test <dbname> <host|default> <hexpayload> <sql> [sql2] ...
 *
 * All statements run on one handle; exits 0 on success, 1 on failure.
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>

extern "C" {
#include <cdb2api.h>
#include <cdb2api_hndl.h>
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    if (argc < 5) {
        fprintf(stderr, "Usage: %s <dbname> <host|default> <hexpayload> <sql> [sql2] ...\n", argv[0]);
        return 1;
    }

    const char *hex = argv[3];
    size_t len = strlen(hex) / 2;
    unsigned char payload[64];
    if (len == 0 || len > sizeof(payload) || strlen(hex) % 2) {
        fprintf(stderr, "bad payload %s\n", hex);
        return 1;
    }
    for (size_t i = 0; i < len; i++)
        sscanf(hex + 2 * i, "%2hhx", &payload[i]);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_hndl_tp *hndl = nullptr;
    int rc = strcmp(argv[2], "default") == 0 ? cdb2_open(&hndl, argv[1], "default", 0)
                                             : cdb2_open(&hndl, argv[1], argv[2], CDB2_DIRECT_CPU);
    if (rc) {
        fprintf(stderr, "cdb2_open failed: %s\n", cdb2_errstr(hndl));
        return 1;
    }

    int failed = 0;
    for (int i = 4; i < argc && !failed; i++) {
        /* cleared at the end of every statement */
        hndl->trace = payload;
        hndl->trace_len = len;
        if ((rc = cdb2_run_statement(hndl, argv[i])) != 0) {
            fprintf(stderr, "[%s] failed with rc %d %s\n", argv[i], rc, cdb2_errstr(hndl));
            failed = 1;
        }
        while (rc == 0 && (rc = cdb2_next_record(hndl)) == CDB2_OK)
            ;
    }

    cdb2_close(hndl);
    return failed;
}
