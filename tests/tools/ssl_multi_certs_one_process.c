#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    cdb2_hndl_tp *charlie = NULL;
    cdb2_hndl_tp *sally = NULL;
    char *conf = getenv("CDB2_CONFIG");
    char *dir = getenv("TESTDIR");
    cdb2_set_comdb2db_config(conf);

    cdb2_open(&charlie, argv[1], "default", 0);
    cdb2_open(&sally, argv[1], "default", 0);

    char stmt[256];
    sprintf(stmt, "SET SSL_CERT %s/charlie.crt", dir);
    cdb2_run_statement(charlie, stmt);
    sprintf(stmt, "SET SSL_KEY %s/charlie.key", dir);
    cdb2_run_statement(charlie, stmt);

    sprintf(stmt, "SET SSL_CERT %s/sally.crt", dir);
    cdb2_run_statement(sally, stmt);
    sprintf(stmt, "SET SSL_KEY %s/sally.key", dir);
    cdb2_run_statement(sally, stmt);

    cdb2_run_statement(charlie, "SELECT * FROM whoami");
    cdb2_next_record(charlie);
    char *charliebrown = strdup((char *)cdb2_column_value(charlie, 0));

    cdb2_run_statement(sally, "SELECT * FROM whoami");
    cdb2_next_record(sally);
    char *sallybrown = strdup((char *)cdb2_column_value(sally, 0));

    if (strcmp(charliebrown, "Charlie Brown") || strcmp(sallybrown, "Sally Brown"))
        return 1;

    puts("SUCCESS");
    return 0;
}
