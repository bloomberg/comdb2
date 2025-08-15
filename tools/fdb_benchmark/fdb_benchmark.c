#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <libgen.h>
#include <unistd.h>

#include <cdb2api.h>

#define DBNAME_LEN 64
#define TYPE_LEN 64
#define POLICY_LEN 24
#define MAX_NODES 128
#define CDB2HOSTNAME_LEN 128
#define MAX_STACK 512 /* Size of call-stack which opened the handle */

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

int value_comparer(cdb2_hndl_tp *hndl) {
    int rc = cdb2_run_statement(hndl, "select * from t");
    if (rc)
        return rc;
    int i = 1;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int val = *(int *)cdb2_column_value(hndl, 0);
        int expected = (i <= 16 || i > 32) ? -i : i;
        if (i > 96)
            expected = i;
        if (val != expected) {
            fprintf(stderr, "Expected %d but got %d\n", expected, val);
            rc = -1;
            return rc;
        }
        i++;
    }
    if (rc != CDB2_OK_DONE)
        return rc;
        
    return 0;
}



int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    cdb2_hndl_tp *hndl;
    int rc;
    hndl = NULL;
    cdb2_hndl_tp *hndl2 = NULL;
    struct cdb2_effects_type effects = {0};
    rc = cdb2_open(&hndl, "testdb", "local", 0);
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }
    rc = cdb2_open(&hndl2, "testdb", "local", 0);
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }

    rc = cdb2_run_statement(hndl, "set continue_on_verify_error on");
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }

    rc = cdb2_run_statement(hndl, "set transaction chunk 16");
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }

    rc = cdb2_run_statement(hndl, "begin");
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }

    rc = cdb2_run_statement(hndl, "update t set a = -a");
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }
    // Note: this is hndl2
    rc = cdb2_run_statement(hndl2, "update t set a = a where a = 17");
    if (rc) {
        printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }
    // rc = cdb2_get_effects(hndl, &effects);
    // if (rc) {
    //     fprintf(stderr, "Error getting effects rc %d\n", rc);
    //     goto done;
    // }
    // rc = effects_comparer(&effects, 100, 0, 100, 0, 0);
    // if (rc) {
    //     goto done;
    // }
    rc = cdb2_get_effects(hndl2, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        goto done;
    }
    rc = effects_comparer(&effects, 1, 0, 1, 0, 0);
    if (rc) {
        goto done;
    }
    printf("Running commit stmt\n");
    rc = cdb2_run_statement(hndl, "commit");
    if (rc != CDB2ERR_VERIFY_ERROR) {
        printf("Didn't get verify error. Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
        goto done;
    }
    rc = cdb2_get_effects(hndl, &effects);
    if (rc) {
        fprintf(stderr, "Error getting effects rc %d\n", rc);
        goto done;
    }
    rc = effects_comparer(&effects, 80, 0, 80, 0, 0);
    // if (rc) {
    //     goto done;
    // }
    rc = value_comparer(hndl);
    if (rc)
        goto done;


    // printf("Running select stmt\n");
    // rc = cdb2_run_statement(hndl, "select 1");
    // if (rc) {
    //     printf("Got rc %d err %s at line %d\n", rc, cdb2_errstr(hndl), __LINE__);
    //     goto done;
    // }
    // while ((rc = cdb2_next_record(hndl)) == CDB2_OK);
    // if (rc != CDB2_OK_DONE) {
    //     printf("Got rc %d err %s\n", rc, cdb2_errstr(hndl));
    //     goto done;
    // }
    goto done;
done:
    cdb2_close(hndl);
    cdb2_close(hndl2);
    
    return rc;
}
