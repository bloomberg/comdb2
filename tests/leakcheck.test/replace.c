#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <cdb2api.h>
#include <epochlib.h>
#include <getopt.h>
#include <errno.h>

static char *argv0=NULL;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f,"     -d <dbname>             -   set dbname.\n" );
    fprintf(f,"     -h                      -   this menu.\n" );
    fprintf(f,"     -i <iters>              -   set iterations.\n" );
    fprintf(f,"     -I                      -   never stop.\n" );
    fprintf(f,"     -n                      -   don't use replaceable params.\n" );
    fprintf(f,"     -s                      -   do replaceble select.\n" );
    fprintf(f,"     -e                      -   cause an error.\n" );
    exit(1);
}

static int cnt=0;
static int iters = 1000;

static inline long long mytimestamp(void)
{
    return (time_epoch() * 1000) + (time_epochms()%1000);
}

void setdata(unsigned char *data, int datasz)
{
    int i;

    for(i = 0 ; i < datasz; i++)
    {
        data[i] = 'a' + (i % 26);
    }
}

/*
 cdb2sql db local "insert into t1(objid, data) values (x'112233445566778899001122', x'1234')"
*/

char *sqlhex(int sz)
{
    static char *s=NULL;
    int i;

    s=malloc( sz * 2 + 1 );

    for( i = 0 ; i < (sz*2) ; i++)
    {
        s[i]='0' + (sz % 10);
    }

    s[sz*2]='\0';
    return s;
}

/* non-replacable params sql- make everything the same size as replaceable params. */
void setsql(char *sql, int len)
{
    snprintf( sql, sizeof(sql),
            "insert into t1(objid, bbgid, timestamp, flags, timestamp2, data, data2) values "
            "(x'112233445566778899001122', x'112233445566778899001122',%lld, 5, %lld, x'%s', x'%s')",  
            mytimestamp(), mytimestamp(), sqlhex(1050), sqlhex(1050));
}

int main(int argc,char *argv[])
{
    int c, err=0, rc = 0, replace=1, isselect=0, doerr=0;
    char *dbname=NULL, sql[4096];
    unsigned char objid[12] = "123456789012", data[1050];
    long long timestamp=0;

    /* Set argv0. */
    argv0 = argv[0];

    /* char *optarg=argument, int optind = argv index  */
    while ((c = getopt(argc,argv,"d:i:Inseh"))!=EOF)
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

            case 'n':
                replace = 0;
                break;

            case 's':
                isselect = 1;
                break;

            case 'I':
                iters = -1;
                break;

            case 'e':
                doerr = 1;
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

    if( isselect && !replace)
    {
        fprintf( stderr, "select test is valid only with replaceable params.\n");
        err++;
    }

    /* Punt before pekludge if there were errors. */
    if( err )
    {
        usage(stderr);
        exit(1);
    }

    cdb2_hndl_tp *sqlh;
    int ret;
    if ((ret = cdb2_open(&sqlh, dbname, "local", 0)) != 0)
    {
        fprintf(stderr, "Error opening db, ret=%d\n", ret);
        exit(1);
    }

    /* Bind objid - test binding with values large and smaller than 12. */
    if(replace && cdb2_bind_param(sqlh, "objid", CDB2_BLOB, objid, sizeof(objid)))
    {
        fprintf(stderr, "Error binding objid.\n");
        exit(1);
    }

    /* Fill data with something predictable. */
    setdata(data, sizeof(data));

    /* Bind the data. */
    if( !isselect && replace && cdb2_bind_param(sqlh, "data", CDB2_BLOB, data, sizeof(data)))
    {
        fprintf(stderr, "Error binding data.\n");
        exit(1);
    }

    long long seltimestamp;
    /* Prepare my sql. */
    if (isselect)
    {
        snprintf( sql, sizeof( sql ), "select objid, bbgid, timestamp, flags, timestamp2, data, data2 from t1 where @objid=objid and timestamp>=%lld LIMIT 1",
                seltimestamp );
    }
    else if (replace)
    {
        snprintf( sql, sizeof( sql ), "insert into t1(objid, data) values(@objid, @data)" );
    }
    else
    {
        snprintf( sql, sizeof(sql),
                "insert into t1(objid, bbgid, timestamp, flags, timestamp2, data, data2) values "
                "(x'112233445566778899001122', x'112233445566778899001122',%lld, 5, %lld, x'%s', x'%s')",  
                mytimestamp(), mytimestamp(), sqlhex(1050), sqlhex(1050));
    }

    /* Have it bind two instead of 1 for my error. */
    if (doerr)
    {
        isselect=0;
    }

    /* Insert in a loop. */
    while( iters < 0 || --iters )
    {
        if( rc = cdb2_run_statement(sqlh, sql) ) {
            fprintf( stderr, "Sql run statement failed: %d %s\n", rc, 
                    cdb2_errstr(sqlh));
            /*exit(1);*/
        }

        /* Read the number of records added. */
        do
        {
            rc = cdb2_next_record(sqlh);

            /* Grab seltimestamp. */
            if( isselect )
            {
                long long *xx = cdb2_column_value(sqlh, 0);
                seltimestamp = *xx;
            }
        }
        while( rc == CDB2_OK );

        /* Bind objid - test binding with values large and smaller than 12. */
        if(replace && cdb2_bind_param(sqlh, "objid", CDB2_BLOB, objid, sizeof(objid)))
        {
            fprintf(stderr, "Error binding objid.\n");
            /*exit(1);*/
        }

        /* Bind the data. */
        if( !isselect && replace && cdb2_bind_param(sqlh, "data", CDB2_BLOB, data, sizeof(data)))
        {
            fprintf(stderr, "Error binding data.\n");
            /*exit(1);*/
        }
    }

    cdb2_close(sqlh);
    return 0;
}
