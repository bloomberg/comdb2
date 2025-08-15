#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <libgen.h>
#include <unistd.h>

#include <cdb2api.h>

int statement_runner(cdb2_hndl_tp *hndl, const char *sql)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc)
        return rc;

    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {}

    if (rc != CDB2_OK_DONE)
        return rc;

    return 0;
}

int effects_comparer(struct cdb2_effects_type *effects, int num_affected, int num_selected, int num_updated, int num_deleted, int num_inserted) {
    if (effects->num_affected != num_affected ||
        effects->num_selected != num_selected ||
        effects->num_updated != num_updated ||
        effects->num_deleted != num_deleted ||
        effects->num_inserted != num_inserted) {
        fprintf(stderr, "Effects mismatch: expected affected=%d, selected=%d, updated=%d, deleted=%d, inserted=%d; got affected=%d, selected=%d, updated=%d, deleted=%d, inserted=%d\n",
                num_affected, num_selected, num_updated, num_deleted, num_inserted,
                effects->num_affected, effects->num_selected, effects->num_updated,
                effects->num_deleted, effects->num_inserted);
        return -1;
    }
    return 0;
}

static int change_throttle(cdb2_hndl_tp *hndl)
{
    char query[500];
    int value = 5000;
    sprintf(query, "put tunable throttle_txn_chunks_msec %d", value);
    int rc = cdb2_run_statement(hndl, query);
    if (rc) {
        fprintf(stderr, "Error setting throttle_txn_chunks_msec to %d %d %s\n", value, rc, cdb2_errstr(hndl));
        return -1;
    }

    // make sure tunable is on/off
    rc = cdb2_run_statement(hndl, "select value from comdb2_tunables where name = 'throttle_txn_chunks_msec'");
    if (rc) {
        fprintf(stderr, "Error running query %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: Expected record %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    char *returned = (char *)cdb2_column_value(hndl, 0);
    char expected[5];
    sprintf(expected, "%d", value);
    if (strcmp(returned, expected)) {
        fprintf(stderr, "Expected tunable %s, got %s\n", expected, returned);
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Expected done %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    return 0;
}

static int tunable_driver(char *dbname)
{
    cdb2_hndl_tp *hndl;
    int rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc) {
        fprintf(stderr, "%s: Error opening %s %d %s\n", __func__, dbname, rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_run_statement(hndl, "select host from comdb2_cluster");
    if (rc) {
        fprintf(stderr, "Error running %s %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    cdb2_hndl_tp *hndl2;
    char *host;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        host = (char *)cdb2_column_value(hndl, 0);
        rc = cdb2_open(&hndl2, dbname, host, CDB2_DIRECT_CPU);
        if (rc) {
            fprintf(stderr, "Error opening %s on host %s %d %s\n", dbname, host, rc, cdb2_errstr(hndl2));
            return -1;
        }
        rc = cdb2_run_statement(hndl2, "select comdb2_host()");
        if (rc) {
            fprintf(stderr, "can't run select comdb2_host on host %s %d %s\n", host, rc, cdb2_errstr(hndl2));
            return -1;
        }
        rc = cdb2_next_record(hndl2);
        if (rc != CDB2_OK) {
            fprintf(stderr, "can't read\n");
            return -1;
        }
        char *host2 = (char *)cdb2_column_value(hndl2, 0);
        if (strcmp(host, host2) != 0) {
            fprintf(stderr, "Should be connected to %s, actually connected to %s\n", host, host2);
            return -1;
        }
        rc = cdb2_next_record(hndl2);
        if (rc != CDB2_OK_DONE) {
            fprintf(stderr, "can't finish reading\n");
            return -1;
        }
        rc = change_throttle(hndl2);
        if (rc)
            return -1;
        cdb2_close(hndl2);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Error reading %s %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_close(hndl);
    if (rc) {
        fprintf(stderr, "%s: Error closing %s %d %s\n", __func__, dbname, rc, cdb2_errstr(hndl));
        return -1;
    }
    return 0;
}

int table_setup(char *dbname) {
    cdb2_hndl_tp *hndl = NULL;
    int rc;

    rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc)
        goto err;
    rc = statement_runner(hndl, "drop table if exists t");
    if (rc)
        goto err;
    rc = statement_runner(hndl, "create table t (a int)");
    if (rc)
        goto err;
    rc = statement_runner(hndl, "insert into t select * from generate_series(1, 100)");
    if (rc)
        goto err;
    goto done;
err:
    fprintf(stderr, "Got rc %d err %s\n", rc, cdb2_errstr(hndl));
done:
    cdb2_close(hndl);
    return rc;
}

int value_comparer(cdb2_hndl_tp *hndl, int divider, int *print_error) {
    int rc = cdb2_run_statement(hndl, "select * from t");
    if (rc)
        return rc;
    int i = 1;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int val = *(int *)cdb2_column_value(hndl, 0);
        int expected = i <= divider ? -i : i;
        if (val != expected) {
            fprintf(stderr, "Expected %d but got %d\n", expected, val);
            rc = -1;
            *print_error = 0;
            return rc;
        }
        i++;
    }
    if (rc != CDB2_OK_DONE)
        return rc;
        
    return 0;
}

int test1(char *dbname) {
    cdb2_hndl_tp *hndl = NULL;
    cdb2_hndl_tp *hndl2 = NULL;
    struct cdb2_effects_type effects = {0};
    int rc;
    int need_to_commit = 0;
    int print_error = 1;

    rc = table_setup(dbname);
    if (rc) {
        print_error = 0;
        goto err;
    }

    rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc)
        goto err;

    rc = cdb2_open(&hndl2, dbname, "default", 0);
    if (rc)
        goto err;
    rc = cdb2_run_statement(hndl, "set transaction chunk 16");
    if (rc)
        goto err;
    rc = cdb2_run_statement(hndl, "set verifyretry off");
    if (rc)
        goto err;
    rc = cdb2_run_statement(hndl, "begin");
    if (rc)
        goto err;
    need_to_commit = 1;
    rc = cdb2_run_statement(hndl, "update t set a = -a");
    if (rc)
        goto err;
    rc = cdb2_get_effects(hndl, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        print_error = 0;
        goto err;
    }
    rc = effects_comparer(&effects, 100, 0, 100, 0, 0);
    if (rc) {
        print_error = 0;
        goto err;
    }

    // Note: this stmt is hndl2
    rc = cdb2_run_statement(hndl2, "update t set a = a where a > 96");
    if (rc)
        goto err;
    rc = cdb2_get_effects(hndl2, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        print_error = 0;
        goto err;
    }
    rc = effects_comparer(&effects, 4, 0, 4, 0, 0);
    if (rc) {
        print_error = 0;
        goto err;
    }

    rc = cdb2_run_statement(hndl, "commit");
    need_to_commit = 0;
    if (rc == 0) {
        print_error = 0;
        fprintf(stderr, "Expected verify error at commit\n");
        rc = -1;
        goto err;
    } else if (rc != CDB2ERR_VERIFY_ERROR)
        goto err;

    rc = cdb2_get_effects(hndl, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        print_error = 0;
        goto err;
    }
    rc = effects_comparer(&effects, 96, 0, 96, 0, 0);
    if (rc) {
        print_error = 0;
        goto err;
    }

    rc = cdb2_run_statement(hndl, "select * from t");
    if (rc)
        goto err;
    rc = value_comparer(hndl, 96, &print_error);
    if (rc)
        goto err;

    rc = 0;
    goto done;

err:
    if (print_error)
        fprintf(stderr, "Got rc %d err %s\n", rc, cdb2_errstr(hndl));
    if (need_to_commit)
        cdb2_run_statement(hndl, "rollback");

done:
    cdb2_close(hndl);
    cdb2_close(hndl2);

    return rc;
}

int test2(char *dbname) {
    cdb2_hndl_tp *hndl = NULL;
    cdb2_hndl_tp *hndl2 = NULL;
    struct cdb2_effects_type effects = {0};
    int rc;
    int need_to_commit = 0;
    int print_error = 0;

    rc = tunable_driver(dbname);
    if (rc)
        goto err;

    rc = table_setup(dbname);
    if (rc)
        goto err;

    print_error = 1;
    rc = cdb2_open(&hndl, dbname, "default", 0);
    if (rc)
        goto err;

    rc = cdb2_open(&hndl2, dbname, "default", 0);
    if (rc)
        goto err;
    rc = cdb2_run_statement(hndl, "set transaction chunk 16");
    if (rc)
        goto err;
    rc = cdb2_run_statement(hndl, "set verifyretry off");
    if (rc)
        goto err;
    rc = cdb2_run_statement(hndl, "begin");
    if (rc)
        goto err;
    need_to_commit = 1;
    rc = cdb2_run_statement(hndl, "update t set a = -a");
    if (rc)
        goto err;

    // Note: this stmt is hndl2
    rc = cdb2_run_statement(hndl2, "update t set a = a where a = 17");
    if (rc)
        goto err;
    rc = cdb2_get_effects(hndl2, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        print_error = 0;
        goto err;
    }
    rc = effects_comparer(&effects, 1, 0, 1, 0, 0);
    if (rc) {
        print_error = 0;
        goto err;
    }

    rc = cdb2_run_statement(hndl, "commit");
    need_to_commit = 0;
    if (rc == 0) {
        print_error = 0;
        fprintf(stderr, "Expected verify error at commit\n");
        rc = -1;
        goto err;
    } else if (rc != CDB2ERR_VERIFY_ERROR)
        goto err;

    rc = cdb2_get_effects(hndl, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        print_error = 0;
        goto err;
    }
    rc = effects_comparer(&effects, 16, 0, 16, 0, 0);
    if (rc) {
        print_error = 0;
        goto err;
    }

    rc = cdb2_run_statement(hndl, "select * from t");
    if (rc)
        goto err;
    rc = value_comparer(hndl, 16, &print_error);
    if (rc)
        goto err;

    rc = 0;
    goto done;

err:
    if (print_error)
        fprintf(stderr, "Got rc %d err %s\n", rc, cdb2_errstr(hndl));
    if (need_to_commit)
        cdb2_run_statement(hndl, "rollback");

done:
    cdb2_close(hndl);
    cdb2_close(hndl2);

    return rc;

}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
        
    char *dbname = argv[1];
    int rc = test1(dbname);
    if (rc)
        abort();
    rc = test2(dbname);
    if (rc)
        abort();
    
    printf("%s - pass\n", basename(argv[0]));
    return 0;
}
