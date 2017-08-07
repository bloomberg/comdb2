#include <stdlib.h>
#include <cdb2api.h>

int main(int argc, char *argv[])
{
    char *dbname=argv[1];
    cdb2_hndl_tp *hndl = NULL;

    int rc = 0;
    if(NULL == dbname)
    {
        fprintf(stderr, "Dbname is not set!\n");
        return -1;
    }

    char * dest = "local";
    char *conf = getenv("CDB2_CONFIG");
    if (conf) {
        cdb2_set_comdb2db_config(conf);
        dest = "default";
    }

    rc = cdb2_open(&hndl, dbname, dest, 0);
    if(rc!=0)
    {
        fprintf(stderr, "error opening sql handle for %s, rc=%d\n", dbname, rc);
        return rc;
    }

    long long int id = 0;

    rc = cdb2_bind_param(hndl, "id", CDB2_INTEGER, &id,  sizeof(long long int));
    if(rc!=0) {
        fprintf(stderr, "error binding id rc=%d \n", rc);
         return rc;
    }

    for (id =0; id < 100; id++) {
       rc = cdb2_run_statement(hndl, "select @id");
       if(rc!=0) {
           fprintf(stderr, "error running sql %d\n", rc);
           return rc;
       }
       rc = cdb2_next_record(hndl);
       if(rc!=0) {
           fprintf(stderr, "error in cdb2_next_record %d\n", rc);
           return rc;
       }
       long long *val = cdb2_column_value(hndl, 0);
       if (*val != id) {
           fprintf(stderr, "error got:%lld expected:%lld\n", *val, id);
           return -1;
       }
    }
    cdb2_clearbindings(hndl);

    rc = cdb2_bind_index(hndl, 1, CDB2_INTEGER, &id,  sizeof(long long int));
    if(rc!=0) {
        fprintf(stderr, "error binding id rc=%d \n", rc);
         return rc;
    }

    for (id =0; id < 100; id++) {
       rc = cdb2_run_statement(hndl, "select ?");
       if(rc!=0) {
           fprintf(stderr, "error running sql %d\n", rc);
           return rc;
       }
       rc = cdb2_next_record(hndl);
       if(rc!=0) {
           fprintf(stderr, "error in cdb2_next_record %d\n", rc);
           return rc;
       }
       long long *val = cdb2_column_value(hndl, 0);
       if (*val != id) {
           fprintf(stderr, "error got:%lld expected:%lld\n", *val, id);
           return -1;
       }
    }
    cdb2_clearbindings(hndl);
    return rc;
}

