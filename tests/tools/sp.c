#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cdb2api.h>

void display_results(cdb2_hndl_tp *hndl) {
    int rc;
    while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
        int num_cols = cdb2_numcolumns(hndl);
        for (int i = 0; i < num_cols; i++) {
            void *val = cdb2_column_value(hndl, i);
            int type = cdb2_column_type(hndl, i);

            printf("%s: ", cdb2_column_name(hndl, i));

            if (val == NULL) {
                printf("NULL");
            } else if (type == CDB2_INTEGER) {
                printf("%lld", *(long long*)val);
            } else if (type == CDB2_REAL) {
                printf("%f", *(double*)val);
            } else if (type == CDB2_CSTRING) {
                printf("'%s'", (char*)val);
            } else if (type == CDB2_BLOB) {
                int len = cdb2_column_size(hndl, i);
                printf("x'");
                for (int j = 0; j < len; j++) {
                    printf("%02x", ((unsigned char*)val)[j]);
                }
                printf("'");
            } else {
                printf("%s", (char*)val);
            }

            if (i < num_cols - 1) printf(", ");
        }
        printf("\n");
    }

    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Error fetching results: %s\n", cdb2_errstr(hndl));
    }
}

void getargs(const char *args, int *id, char **str) { //currently only supports 2 args: int, string
    char *args_copy = strdup(args); 
    char *token = strtok(args_copy, ",");
    if (token) {
        *id = atoi(token);
        token = strtok(NULL, ",");
        if (token) {
            *str = strdup(token);
            //strip quotes if present
            size_t len = strlen(*str);
            if (len >= 2 && (((*str)[0] == '\'' && (*str)[len - 1] == '\'') ||
                            ((*str)[0] == '\"' && (*str)[len - 1] == '\"'))) {
                memmove(*str, *str + 1, len - 2);
                (*str)[len - 2] = '\0';
            }

        }
    }
    free(args_copy);
}

int exec_procedure_with_bind(cdb2_hndl_tp *hndl, const char *table, const char *spname, int id, const char *str) {
    char sql[256];
    
    printf("\n=== 1: calling exec procedure with bind parameters ===\n");
    size_t len = str != NULL ? strlen(str) : 0;
    int rc = cdb2_bind_param(hndl, "val", CDB2_CSTRING, str, len);
    if (rc != 0) {
        fprintf(stderr, "Failed to bind val parameter: %s\n", cdb2_errstr(hndl));
        return rc;
    }
    rc = cdb2_bind_param(hndl, "id", CDB2_INTEGER, &id, sizeof(id));
    if (rc != 0) {
        fprintf(stderr, "Failed to bind id parameter: %s\n", cdb2_errstr(hndl));
        return rc;
    }

    snprintf(sql, sizeof(sql), "EXEC PROCEDURE %s(@val, @id)", spname);
    printf("Executing SQL: %s\n", sql);
    rc = cdb2_run_statement(hndl, sql);
    if (rc != 0) {
        fprintf(stderr, "Failed to execute procedure with bind params: %s\n", cdb2_errstr(hndl));
        return rc;
    }

    printf("Stored procedure results with bind parameters:\n");
    cdb2_clearbindings(hndl);
    display_results(hndl);

    snprintf(sql, sizeof(sql), "SELECT * FROM %s where id=%d", table, id);
    printf("\n=== Querying %s: sql: %s ===\n", table, sql);
    rc = cdb2_run_statement(hndl, sql);
    if (rc != 0) {
        fprintf(stderr, "Failed to query t1: %s\n", cdb2_errstr(hndl));
        return 1;
    }
    display_results(hndl);
    
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <dbname> <tier> <table> <spname> <args>\n", argv[0]);
        return 1;
    }

    const char *dbname = argv[1];
    const char *tier = argv[2];
    const char *table = argv[3];
    const char *spname = argv[4];
    const char *args = argv[5];
    const char *cfg = argc == 7 ? argv[6] : NULL;

    if (cfg) {
        cdb2_set_comdb2db_config(cfg);
    }
    
    int id;
    char *str;
    getargs(args, &id, &str);
    printf("Parsed arguments: id=%d, str='%s'\n", id, str);
    cdb2_hndl_tp *hndl = NULL;

    int rc = cdb2_open(&hndl, dbname, tier, 0);
    if (rc != 0) {
        fprintf(stderr, "Failed to connect to database: %s\n", cdb2_errstr(hndl));
        return 1;
    }

    rc = exec_procedure_with_bind(hndl, table, spname, id, str);
    if (rc != 0) {
        cdb2_close(hndl);
        free(str);
        return 1;
    }

    rc = exec_procedure_with_bind(hndl, table, spname, id, NULL);
    if (rc != 0) {
        cdb2_close(hndl);
        free(str);
        return 1;
    }

    free(str);
    cdb2_close(hndl);
    printf("\nTest completed successfully!\n");
    return 0;
}
