#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <cdb2api.h>

int print_results(int check, cdb2_hndl_tp *db) {
    if (check) {
        fprintf(stderr, "Error fetching results: %d %s\n", check, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    int rc;
    printf("Results from procedure:\n");
    while ((rc = cdb2_next_record(db)) == CDB2_OK) {
        int ncols = cdb2_numcolumns(db);
        printf("Row: ");
        for (int i = 0; i < ncols; i++) {
            void *val = cdb2_column_value(db, i);
            int type = cdb2_column_type(db, i);
            const char *colname = cdb2_column_name(db, i);
            
            printf("%s=", colname);
            
            switch (type) {
                case CDB2_INTEGER:
                    printf("%lld", *(long long *)val);
                    break;
                case CDB2_REAL:
                    printf("%f", *(double *)val);
                    break;
                case CDB2_CSTRING:
                    printf("\"%s\"", (char *)val);
                    break;
                case CDB2_BLOB:
                    printf("<blob:%d bytes>", cdb2_column_size(db, i));
                    break;
                default:
                    printf("<type:%d>", type);
                    break;
            }
            
            if (i < ncols - 1) {
                printf(", ");
            }
        }
        printf("\n");
    }
    return 0;
}

int main(int argc, char *argv[]) {
    cdb2_hndl_tp *db;
    int rc;
    char *dbname;
    char *tier;

    if (argc != 3) {
        printf("Usage: %s <dbname> <tier>\n", argv[0]);
        return 1;
    }
    
    dbname = argv[1];
    tier = argv[2];
    
    char *config = getenv("CDB2_CONFIG");
    if (config) {
        cdb2_set_comdb2db_config(config);
    }
    
    rc = cdb2_open(&db, dbname, tier, 0);
    if (rc) {
        fprintf(stderr, "Error opening database: %d %s\n", rc, cdb2_errstr(db));
        return 1;
    }
    
    rc = cdb2_run_statement(db, "CREATE PROCEDURE csvdemo VERSION 'test' { "
        "local function main(c) "
        "    local array = db:csv_to_table(c) "
        "    local freq = {} "
        "    for i = 1, #array do "
        "        array[i] = db:cast(array[i], 'cstring') "
        "        freq[i] = 0 "
        "        for _ in string.gmatch(array[i], '.') do "
        "            freq[i] = freq[i] + 1 "
        "        end "
        "    end "
        "   local result = {} "
        "    for i = 1, #array do "
        "       db:column_name('result[' .. i .. ']', i) "
        "       result[i] = array[i] .. ' (' .. freq[i] .. ')' "
        "    end "
        "   print(result)"
        "    db:emit(unpack(result)) "
        "end "
        "}");
    if (rc) {
        fprintf(stderr, "Error creating procedure: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    printf("Procedure created successfully!\n");

    rc = cdb2_run_statement(db, "CREATE PROCEDURE estr VERSION 'test' {"
    "local function main(x)"
    "   local s = db:cast(x, 'cstring')"
    "   db:column_name('x', 1) db:column_name('s', 2)"
    "   db:emit({x = x, s = s})"
    "end"
    "}"
    );

    const char *csv_str = "a,,\"hello, world!\",\"is \"\"quoted\"\" back there\",";
    rc = cdb2_bind_param(db, "csv", CDB2_CSTRING, csv_str, strlen(csv_str));
    if (rc) {
        fprintf(stderr, "Error binding parameter: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }

    // const char *x = "";
    rc = cdb2_run_statement(db, "exec procedure csvdemo(@csv)");
    if (rc) {
        fprintf(stderr, "Error executing procedure: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    
    if (print_results(rc == CDB2_OK_DONE, db)) {
        return 1;
    }

    const char *x = "";
    rc = cdb2_bind_param(db, "x", CDB2_CSTRING, x, strlen(x));
    if (rc) {
        fprintf(stderr, "Error binding parameter: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }

    rc = cdb2_run_statement(db, "exec procedure estr(@x)");
    if (rc) {
        fprintf(stderr, "Error executing procedure: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    if (print_results(rc == CDB2_OK_DONE, db)) {
        return 1;
    }

    rc = cdb2_run_statement(db, "exec procedure estr('')");
    if (rc) {
        fprintf(stderr, "Error executing procedure: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    if (print_results(rc == CDB2_OK_DONE, db)) {
        return 1;
    }

    rc = cdb2_run_statement(db, "exec procedure estr('@x')");
    if (rc) {
        fprintf(stderr, "Error executing procedure: %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return 1;
    }
    if (print_results(rc == CDB2_OK_DONE, db)) {
        return 1;
    }
    
    printf("Procedure executed successfully!\n");
    
    cdb2_close(db);
    return 0;
}
