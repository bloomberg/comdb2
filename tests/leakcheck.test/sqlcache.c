#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <cdb2api.h>
#include <epochlib.h>
#include <errno.h>
#include <unistd.h>

static char *argv0=NULL;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f,"     -d <dbname>             -   set dbname.\n" );
    fprintf(f,"     -h                      -   this menu.\n" );
    fprintf(f,"     -i <iters>              -   set iterations.\n" );
    fprintf(f,"     -I                      -   never stop.\n" );
    exit(1);
}

static int cnt=0;
static int iters = 1000;

long long get_timestamp(void)
{
    return time_epoch() * 1000 + time_epochms();
}

void setdata(unsigned char *data, int datasz)
{
    int i;

    for(i = 0 ; i < datasz; i++)
    {
        data[i] = 'a' + (i % 26);
    }
}

int main(int argc,char *argv[])
{
    int c, err=0, rc = 0;
    char *dbname=NULL, sql[256];
    unsigned char objid[12] = "123456789012", data[1050];
    cdb2_hndl_tp *sqlh;

    /* Set argv0. */
    argv0 = argv[0];

    /* char *optarg=argument, int optind = argv index  */
    while ((c = getopt(argc,argv,"d:i:Ih"))!=EOF)
    {
        switch(c)
        {
            case 'd':
                dbname = optarg;
                break;

            case 'h':
                usage(stdout);
                break;

            case 'i':
                iters = atoi(optarg);
                break;

            case 'I':
                iters = -1;
                break;

            default:
                fprintf(stderr, "Unknown flag, '%c'.\n", optopt);
                err++;
                break;
        }
    }

    /* Make sure dbname is set. */
    if( NULL == dbname)
    {
        fprintf( stderr, "dbname is unset.\n");
        err++;
    }

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    /* Allocate an sql handle */
    if(cdb2_open(&sqlh, dbname, "default", CDB2_RANDOM))
    {
        fprintf(stderr, "Error allocating an sql handle.\n");
        exit(1);
    }

    /* Punt if there were errors. */
    if( err )
    {
        usage(stderr);
        exit(1);
    }

    /* Fill data with something predictable. */
    setdata(data, sizeof(data));

    /* Insert in a loop. */
    while( iters < 0 || --iters )
    {
        cdb2_clearbindings(sqlh);
        /* Prepare my sql. */
        snprintf(sql, sizeof(sql), "insert into t1(objid, data, timestamp, i) "
                                   "values(@objid, @data, %lld, %d)",
                 get_timestamp(), iters);

        /* Bind objid - test binding with values large and smaller than 12. */
        if( cdb2_bind_param( sqlh, "objid", CDB2_BLOB, objid, sizeof( objid ) ) )
        {
            fprintf(stderr, "Error binding objid.\n");
            exit(1);
        }

        /* Bind the data. */
        if( cdb2_bind_param( sqlh, "data", CDB2_BLOB, data, sizeof( data ) ) )
        {
            fprintf(stderr, "error inserting record rc=%d %s.\n", rc, cdb2_errstr(sqlh));
            exit(1);
        }

        if ((rc = cdb2_run_statement(sqlh, sql)) != 0)
        {
            fprintf( stderr, "Sql run statement failed: %d %s\n", rc, 
                    cdb2_errstr(sqlh));
            exit(1);
        }

        /* Read the number of records added. */
        do
        {
            rc = cdb2_next_record(sqlh);
        }
        while( rc == CDB2_OK );
    }

    return 0;
}

