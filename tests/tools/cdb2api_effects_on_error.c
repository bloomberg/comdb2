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

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);
        
    char *dbname = argv[1];
    cdb2_hndl_tp *hndl = NULL;
    cdb2_hndl_tp *hndl2 = NULL;
    struct cdb2_effects_type effects = {0};
    int rc;
    int need_to_commit = 0;
    int print_error = 1;

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
    int i = 1;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int val = *(int *)cdb2_column_value(hndl, 0);
        int expected = i <= 96 ? -i : i;
        if (val != expected) {
            fprintf(stderr, "Expected %d but got %d\n", expected, val);
            rc = -1;
            print_error = 0;
            goto err;
        }
        i++;
    }
    if (rc != CDB2_OK_DONE)
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

    if (rc)
        abort();
    
    printf("%s - pass\n", basename(argv[0]));
    return 0;
}
