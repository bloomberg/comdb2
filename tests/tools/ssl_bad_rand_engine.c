/* This test verifies that the API behaves as expected
 * even when a broken RAND engine is in the system */
#define OPENSSL_API_COMPAT 0x10020000L
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <libgen.h>
#include <limits.h>
#include <openssl/engine.h>

#include <cdb2api.h>

static int can_call_bad_rand = 1;
static int bad_rand(unsigned char *buf, int num)
{
    if (!can_call_bad_rand)
        abort();
    return 0;
}

static int bad_rand_status(void)
{
    return 1;
}

static int rand_init(ENGINE *unused)
{
    return 1;
}

int main(int argc, char **argv)
{
    char *db, *tier;
    char *conf = getenv("CDB2_CONFIG");
    if (argc < 2)
        exit(1);

    db = argv[1];

    if (argc > 2)
        tier = argv[2];
    else
        tier = "default";

    if (conf != NULL)
        cdb2_set_comdb2db_config(conf);

    cdb2_hndl_tp *hndl = NULL;
    int rc;
    RAND_METHOD bad_rand_method;
    memset(&bad_rand_method, 0, sizeof(RAND_METHOD));
    bad_rand_method.bytes = bad_rand;
    bad_rand_method.pseudorand = bad_rand;
    bad_rand_method.status = bad_rand_status;

    ENGINE *e = ENGINE_new();
    ENGINE_set_id(e, "cdb2test");
    ENGINE_set_name(e, "bad rand engine for cdb2api testsuite");
    ENGINE_set_flags(e, ENGINE_FLAGS_NO_REGISTER_ALL);
    ENGINE_set_init_function(e, rand_init);
    ENGINE_set_RAND(e, &bad_rand_method);

    ENGINE_add(e);

    ENGINE_init(e);
    ENGINE_set_default(e, ENGINE_METHOD_RAND);

    ENGINE_remove(e);
    ENGINE_finish(e);
    ENGINE_free(e);
    ENGINE_cleanup();
    RAND_set_rand_engine(NULL);
    RAND_set_rand_method(NULL);

    setenv("SSL_MODE", "REQUIRE", 1);
    rc = cdb2_open(&hndl, db, tier, 0);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    if (rc == 0) {
        fprintf(stderr, "rand engine is broken; ssl-require should not have succeeded\n");
        return -1;
    }
    printf("this error is expected: %s\n", cdb2_errstr(hndl));
    cdb2_close(hndl);

    setenv("SSL_MODE", "PREFER", 1);
    rc = cdb2_open(&hndl, db, tier, 0);
    rc = cdb2_run_statement(hndl, "SELECT 1");
    if (rc != 0) {
        fprintf(stderr, "rand engine is broken; ssl-prefer should still have succeeded\n");
        printf("perfer error: %s\n", cdb2_errstr(hndl));
        return -1;
    }

    ENGINE_unregister_RAND(e);
    RAND_set_rand_method(NULL);
    can_call_bad_rand = 0;
    unsetenv("SSL_MODE");
    return 0;
}
