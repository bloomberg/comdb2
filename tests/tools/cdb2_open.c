#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdio.h>
#include <cdb2api.h>
#include <gettimeofday_ms.h>

#define DEFAULT_ITERATIONS 200000

static char *argv0 = NULL;

void usage(void)
{
    fprintf(stderr, "Usage: %s -d <dbname> [-i <iter> ] [-c <config>]\n", argv0);
    exit(1);
}

int main(int argc, char *argv[])
{
    int iterations = DEFAULT_ITERATIONS, err = 0, i, c;
    cdb2_hndl_tp *cdb2 = NULL;
    uint64_t start, end;
    char *dbname = NULL;

    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:c:i:")) != EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 'i':
                iterations = atoi(optarg);
                break;
            case 'c':
                cdb2_set_comdb2db_config(optarg);
                break;
            default:
                fprintf(stderr, "Unknown option, '%c'\n", c);
                err++;
                break;
        }
    }
    if (!dbname) {
        fprintf(stderr, "Dbname is unset\n");
        err++;
    }

    if (err)
        usage();

    start = gettimeofday_ms();
    for (i = 0 ; i < iterations ; i++) {
        cdb2_open(&cdb2, dbname, "local", 0);
        cdb2_close(cdb2);
    }
    end = gettimeofday_ms();
    int tot = end - start;
    int secs = (end - start) / 1000;

    printf("Open/closed %d times in %d ms, %d per second\n",
            iterations, tot, secs ? iterations / secs : 0);
    return 0;
}
