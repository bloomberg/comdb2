#include <stdio.h>
#include <stdlib.h>
#include <cdb2api.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>

static char *argv0=NULL;
static cdb2_hndl_tp *sqlh=NULL;
void md5_add(const char *z);

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n",argv0);
    fprintf(f,"     -i <iterations>             iterate\n");
    fprintf(f,"     -d <dbname>                 set dbname to <dbname>\n");
    fprintf(f,"     -s <startnum>               set the start number\n");
    fprintf(f,"     -l <len>                    set bloblen\n");
    fprintf(f,"     -h                          print this help menu\n");
    exit(1);
}

/* returns a hex-blob which is sz*2 characters long */
static char *makeblob(int sz)
{
    char *r;
    int x;
    static char hx[] = 
    { 
        '0', '1', '2', '3', 
        '4', '5', '6', '7', 
        '8', '9', 'a', 'b', 
        'c', 'd', 'e', 'f'
    };

    sz*=2;

    r=(char *)malloc(sz + 1);
    if(!r) 
    { 
        fprintf(stderr,"%s: can't allocate mem\n",__func__); 
        exit(1); 
    }

    for(x=0;x<sz;x++)
    {
        r[x] = hx[ x % sizeof(hx) ];
    }

    r[sz]='\0';
    return r;
}

int main(int argc,char *argv[])
{
    int rc, c, i=0,j,iter=100, err=0, bloblen = 21042, startnum=0, numcols, sqllen;
    char *blb, *sql=NULL, *dbname=NULL;
    argv0=argv[0];


    /* char *optarg=argument, int optind = argv index  */
    while ((c = getopt(argc,argv,"i:d:s:l:h"))!=EOF)
    {
        switch(c)
        {
            case 'i':
                iter=atoi(optarg);
                break;
            case 'd':
                dbname=optarg;
                break;
            case 'h':
                usage(stdout);
                break;
            case 'l':
                bloblen=atoi(optarg);
                break;
            case 's':
                startnum = atoi(optarg);
                break;
            default:
                fprintf(stderr,"unknown option, '%c'\n", c);
                err++;
        }
    }

    if(NULL == dbname)
    {
        fprintf(stderr, "dbname is not set!\n");
        err++;
    }

    if(err) usage(stderr);
    srand( time(NULL) * getpid() );

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&sqlh, dbname, "default", 0);
    if(0 != rc)
    {
        fprintf(stderr,"error allocating sqlhandle\n");
        exit(1);
    }

    blb = makeblob( bloblen );

    if(!blb)
    {
        fprintf(stderr,"makeblob returns NULL\n");
        exit(1);
    }

    /* allocate buffer for sql (with fudge!) */
    sqllen = 100 + (bloblen * 2) ;
    sql = malloc( sqllen );
    if(!sql) {
        fprintf(stderr,"can't malloc %d bytes\n", sqllen);
        exit(1);
    }    

    /* update blobs in a loop */
    for( i = 0 ; i < iter ; i++ )
    {
        snprintf( sql, sqllen, 
                "insert into blobtest1(seqno, blob1)  values (%d, x'%s')", 
                startnum++, blb );

        rc = cdb2_run_statement( sqlh, sql );
        if(0 != rc)
        {
            fprintf(stderr,"cdb2_run_statement returns %d: %s\n", rc, 
                    cdb2_errstr( sqlh ) );
            /*exit(1);*/
        }

        numcols = cdb2_numcolumns( sqlh );

        rc = cdb2_next_record( sqlh );

        while(CDB2_OK == rc)
        {
            for( j = 0 ; j < numcols ; j++ )
            {
                int type = cdb2_column_type(sqlh, j);

                if(CDB2_CSTRING == type)
                {
                    char *val = (char *)cdb2_column_value(sqlh, j);
                    printf("iter #%d,  col='%s' val=%s\n", i, cdb2_column_name(sqlh, j),
                            val);
                }
                else if(CDB2_INTEGER == type)
                {
                    long long val = *(long long *)cdb2_column_value(sqlh, j);
                    printf("iter #%d,  col='%s' val=%lld\n", i, cdb2_column_name(sqlh, j),
                            val);
                }

            }
            rc = cdb2_next_record( sqlh );
        }
    }

    return 0;
}

