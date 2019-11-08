#include <stdio.h>
#include <stdlib.h>
#include <cdb2api.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>

static char *argv0=NULL;
static char *dbname=NULL;
static int id=1;
static int pinterval = 1000;
static int send_error_pragma = 0;

void usage(FILE *f)
{
    fprintf(f,"Usage: %s <cmd-line>\n", argv0);
    fprintf(f," -d <dbname>         - sets dbname (defaults to clobsef)\n");
    fprintf(f," -i <id>             - sets bookid defaults to 1\n");
    fprintf(f," -I                  - don't do inial insert\n");
    fprintf(f," -p <interval>       - set print-interval\n");
    fprintf(f," -m <maxseq>         - set maximum seqnum\n");
    fprintf(f," -e                  - send error pragma on error\n");
    fprintf(f," -h                  - this menu\n");
    exit(1);
}

int main(int argc, char *argv[])
{
    int err=0, opt, rc, ret, seq, maxseq=100000, stime, etime;
    long long updcnt;
    char sql[1000];
    cdb2_hndl_tp *hndl = NULL;
    argv0=argv[0];

    while ((opt = getopt(argc,argv,"d:i:p:m:eh"))!=EOF)
    {
        switch(opt)
        {
            case 'd':
                dbname=optarg;
                break;

            case 'p':
                pinterval = atoi(optarg);
                break;

            case 'i':
                id=atoi(optarg);
                break;

            case 'm':
                maxseq = atoi(optarg);
                break;

            case 'e': 
                send_error_pragma = 1;
                break;

            case 'h':
                usage(stdout);
                break;

            default:
                fprintf(stderr, "Unknown option, '%c'.\n", opt);
                err++;
                break;
        }
    }

    if(NULL == dbname)
    {
        fprintf(stderr, "Dbname is not set!\n");
        err++;
    }

    if(err)
    {
        usage(stderr);
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
        usage(stderr);
    }

    snprintf(sql, sizeof(sql), "SET TRANSACTION SNAPSHOT ISOLATION");

    if((ret = cdb2_run_statement(hndl, sql)) != 0)
    {
        fprintf(stderr, "error running sql at line %d.\n", __LINE__);
        exit(1);
    }
    do
    {
        ret = cdb2_next_record(hndl);
    } while(ret == CDB2_OK);

    snprintf(sql, sizeof(sql), "SET VERIFYRETRY OFF");

    if((ret = cdb2_run_statement(hndl, sql)) != 0)
    {
        fprintf(stderr, "error running sql at line %d.\n", __LINE__);
        exit(1);
    }
    do
    {
        ret = cdb2_next_record(hndl);
    } while(ret == CDB2_OK);

    {
        snprintf(sql, sizeof(sql), "INSERT into book(id, high_water_sequence) values (%d, 1)", id);

        if((ret = cdb2_run_statement(hndl, sql)) != 0)
        {
            fprintf(stderr, "error running sql %s at line %d with ret %d; %s.\n",
                    sql, __LINE__, ret, cdb2_errstr(hndl));
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(hndl);
        } while(ret == CDB2_OK);
    }

    /*
    if((ret = cdb2_bind_param(hndl, "book_id", CDB2_INTEGER, &id,  sizeof(int64_t)))!=0)
    {
        fprintf(stderr, "error binding id, ret=%d\n", ret);
        exit(1);
    }

    snprintf(sql, sizeof(sql), "UPDATE book SET high_water_sequence = 1 where id = @book_id");

    if((ret = cdb2_run_statement(hndl, sql)) != 0)
    {
        fprintf(stderr, "error running sql at line %d.\n", __LINE__);
        exit(1);
    }
    do
    {
        ret = cdb2_next_record(hndl);
    } while(ret == CDB2_OK);

    cdb2_clearbindings(hndl);
    */

    for(seq = 2 ; seq < maxseq ; seq++)
    {
        /* BEGIN */
        if((ret = cdb2_run_statement(hndl, "BEGIN")) != 0)
        {
            fprintf(stderr, "error running sql at line %d.\n", __LINE__);
            exit(1);
        }
        do
        {
            ret = cdb2_next_record(hndl);
        } while(ret == CDB2_OK);



        snprintf( sql, sizeof( sql ), "select count(*) from book where id = %d AND high_water_sequence = %d", id, seq-1);
        if((ret = cdb2_run_statement(hndl, sql)) != CDB2_OK)
        {
            fprintf(stderr, "error running sql %s at line %d with ret %d; %s.\n",
                    sql, __LINE__, ret, cdb2_errstr(hndl));
            exit(1);
        }

        while( (ret = cdb2_next_record(hndl)) == CDB2_OK) {
            void *xx = cdb2_column_value(hndl, 0);
            printf("COUNT IS %d.\n", *(int*) xx);
        } 


        /* BIND */
        /*
        if((ret = cdb2_bind_param(hndl, "book_id", CDB2_INTEGER, &id, sizeof(int64_t)))!=0)
        {
            fprintf(stderr, "error binding id, ret=%d\n", ret);
            exit(1);
        }

        int oldseq = seq - 1;
        if((ret = cdb2_bind_param(hndl, "old_seq", CDB2_INTEGER, &oldseq, sizeof(int64_t)))!=0)
        {
            fprintf(stderr, "error binding old_seq, ret=%d\n", ret);
            exit(1);
        }

        if((ret = cdb2_bind_param(hndl, "new_seq", CDB2_INTEGER, &seq, sizeof(int64_t)))!=0)
        {
            fprintf(stderr, "error binding new_seq, ret=%d\n", ret);
            exit(1);
        }
        while((ret = cdb2_next_record(hndl)) == CDB2_OK);
        */

        /* UPDATE */
        //const char * updstmt="UPDATE book SET high_water_sequence = @new_seq WHERE id = @book_id AND high_water_sequence = @old_seq";
        snprintf( sql, sizeof( sql ), "UPDATE book SET high_water_sequence = %d WHERE id = %d AND high_water_sequence = %d", seq, id, seq-1);

        if((ret = cdb2_run_statement(hndl, sql)) != 0)
        {
            fprintf(stderr, "error running sql %s at line %d with ret %d; %s.\n",
                    sql, __LINE__, ret, cdb2_errstr(hndl));
            exit(1);
        }


        cdb2_effects_tp ef;
        rc = cdb2_get_effects(hndl, &ef);
        if (rc) {
            fprintf(stderr, "cdb2_get_effects rc %d, %s\n", rc,  cdb2_errstr(hndl));
            exit(1);
        }
        //printf("UPD s %d, u %d, d %d, i %d\n",  ef.num_selected , ef.num_updated , ef.num_deleted , ef.num_inserted);

        //cdb2_clearbindings(hndl);
        updcnt = ef.num_updated;

        if(updcnt != 1)
        {
            fprintf(stderr, "invalid update count on iteration %d, updcnt = %lld id=%d old_seq=%d\n", seq, updcnt, id, seq - 1);
            if(send_error_pragma)
            {
                snprintf(sql, sizeof(sql), "SET error GOT 0 UPDATES, SHOULD HAVE GOTTEN 1!");
                if((ret = cdb2_run_statement(hndl, sql)) != 0)
                {
                    fprintf(stderr, "Error %d sending set error pragma\n", ret);
                }
            }

            exit(1);
        }

        /* COMMIT */
        stime = time(NULL);

        if((ret = cdb2_run_statement(hndl, "COMMIT")) != 0)
        {
            fprintf(stderr, "error running sql at line %d.\n", __LINE__);
            exit(1);
        }
        etime = time(NULL);
        do
        {
            ret = cdb2_next_record(hndl);
        } while(ret == CDB2_OK);

        if(etime - stime > 1)
        {
            fprintf(stderr, "Long runtime for commit: %d seconds!  iteration %d, id = %d\n", 
                    etime - stime, seq, id);
        }

        if(pinterval > 0 && 0 == (seq) % pinterval)
        {
            fprintf(stderr, "Completed iteration %d\n", seq);
        }
    }
    return 0;
}

