/*
   Simple program to time a blob insertion.
   Run 3 times and return the shortest runtime measured in microseconds.
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <limits.h>

#include <cdb2api.h>

int main(int argc, char **argv)
{
    int rc;

    if (argc != 4) {
        puts("invalid arguments");
        return -1;
    }

    char *dbname = argv[1];
    char *mach = argv[2];
    int size = atoi(argv[3]);
    void *buf = calloc(1, size);

    cdb2_hndl_tp *hndl = NULL;
 
    rc = cdb2_open(&hndl, dbname, mach, CDB2_DIRECT_CPU);
    if (rc != 0) {
        puts("cdb2_open");
        goto done;
    }
    rc = cdb2_run_statement(hndl, "DROP TABLE IF EXISTS bloby");
    if (rc != 0) {
        puts("run_statement('DROP TABLE bloby')");
        goto done;
    }
    rc = cdb2_run_statement(hndl, "CREATE TABLE bloby (data BLOB)");
    if (rc != 0) {
        puts("run_statement('CREATE TABLE bloby')");
        goto done;
    }
    cdb2_bind_param(hndl, "data", CDB2_BLOB, buf, size);
    rc = cdb2_run_statement(hndl, "INSERT INTO bloby VALUES (@data)");
    if (rc != 0) {
        puts("run_statement('INSERT INTO bloby')");
        goto done;
    }
    cdb2_clearbindings(hndl);

    long duration = LONG_MAX;

    for (int i = 0; i != 3; ++i) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        long usec_then = tv.tv_sec * 1000000L + tv.tv_usec;
        rc = cdb2_run_statement(hndl, "UPDATE bloby SET data = data WHERE 1");
        if (rc != 0) {
            puts("run_statement('UPDATE bloby')");
            goto done;
        }
        gettimeofday(&tv, NULL);
        long usec_now = tv.tv_sec * 1000000L + tv.tv_usec;

        if (usec_now - usec_then < duration)
            duration = usec_now - usec_then;
    }

    printf("%ld\n", duration);

done:
    free(buf);
    cdb2_close(hndl);
    return rc;
}
