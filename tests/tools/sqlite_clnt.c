#include <stdio.h>
#include <cdb2api.h>
#include <sqlite3.h>
#include <arpa/inet.h>

void usage(FILE *f, char *argv0) {
    fprintf(stderr, "Usage: %s <dbname> <tblname>\n", argv0);
    exit(1);
}

int process_sqlite_row(const unsigned char *row, int rowlen);

int main(int argc, char **argv)
{
    cdb2_hndl_tp *hndl = NULL;
    int rc;
    
    if (argc != 3)
        usage(stderr, argv[0]);

    char *dbname = argv[1];
    char *tblname = argv[2];

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&hndl, dbname, /*"default"*/"local", CDB2_SQL_ROWS);
    if (rc) {
        fprintf(stderr, "Failed to open db %s rc %d\n", dbname, rc);
        exit(1);
    }

    char *sql = sqlite3_mprintf("select * from %s order by 1", tblname);
    if (!sql) {
        fprintf(stderr, "Failed to allocate sql string\n");
        exit(1);
    }

    rc = cdb2_run_statement(hndl, sql);
    if (rc) {
        fprintf(stderr, "Failed to run query rc %d \"%s\"\n", rc, sql);
        free(sql);
        cdb2_close(hndl);
        exit(1);
    }


    int ncols = cdb2_numcolumns(hndl);
    if (ncols != 1) {
        fprintf(stderr, "Error: wrong number of columns %d, expected 1\n", ncols);
        exit(1);
    }

    rc = cdb2_next_record(hndl);
    int i = 1;
    while (rc == CDB2_OK) {
        const unsigned char *row = cdb2_column_value(hndl, 0);
        int rowlen = cdb2_column_size(hndl, 0);

        /*fprintf(stderr, "Got sqlite row length %d %p\n", rowlen, row);*/
        rc = process_sqlite_row(row, rowlen);
        if (rc) {
            fprintf(stderr, "Failed to process row %d rc %d\n", i, rc);
        }

        rc = cdb2_next_record(hndl);
        i++;
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Failing to return all rows rc %d\n", rc);
        exit(1);
    }

    cdb2_close(hndl);

    return 0;
}

int process_sqlite_row(const unsigned char *row, int rowlen)
{
    /* header size has to be a byte */
    if (row[0] & 0x80) 
        return -1;
    int hdrSz = row[0];

    /* simple format only: hdrsz(u8) type1(u8) type2(u8) */
    if (hdrSz != 3)
        return -1; 

    int type1 = row[1];
    int type2 = row[2];

    /* first field should be int */
    if (type1 != 6)
        return -1;
    
    int val = htonl(*(int*)&row[hdrSz]);
    val <<= 16;
    val += htonl(*(int*)&row[hdrSz+4]);

    /* second field should be a small string */
    if (type2 < 12 || !(type2 & 0x01))
        return -1;
    
    int lstr = (type2-12)/2;

    fprintf(stdout, "(%d, \'%.*s\')\n", val, lstr, &row[hdrSz+8]);

    return 0;    
}
