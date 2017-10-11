#include <alloca.h>
#include <stdarg.h>
#include <strings.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>
#include <poll.h>
#include <stdint.h>
#include <sys/time.h>
#include <string.h>
#include <ctype.h>
#include <signal.h>
#include <assert.h>
#include <testutil.h>
#include <nemesis.h>
#include <cdb2api.h>

enum eventtypes {
    PARTITION_EVENT = 0x00000001,
    SIGSTOP_EVENT = 0x00000002,
    CLOCK_EVENT = 0x00000004
};

int runtime = 0;
int max_clock_skew = 60;

char *dbname = NULL;
char *cltype = "dev";
char *argv0 = NULL;

uint32_t which_events = 0;
int partition_master = 1;
int partition_whole_network = 1;
int max_retries = 1000000;
int debug_trace = 0;
int nemesis_length = 10;
int sleep_time = 30;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s [ opts ]\n", argv0);
    fprintf(f, "        -d <dbname>         - set name of the test database\n");
    fprintf(f, "        -c <config>         - set cdb2cfg file\n");
    fprintf(f, "        -G <event>          - add event type ('partition', "
               "'sigstop', or 'clock')\n");
    fprintf(
        f, "        -t <cltype>         - 'dev', 'alpha', 'beta', or 'prod'\n");
    fprintf(f, "        -M                  - partition the master\n");
    fprintf(f, "        -W                  - partition by port\n");
    fprintf(f, "        -D                  - enable debug trace\n");
    fprintf(f, "        -r <runtime>        - set runtime in seconds\n");
    fprintf(f, "        -n <nemesis-length> - length of nemesis event\n");
    fprintf(f, "        -s <sleep-time>     - sleep between nemesis\n");
}

int main(int argc, char *argv[])
{
    int c, errors = 0, cnt;
    uint32_t flags = 0, start_time;
    struct nemesis *n = NULL;

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:c:G:t:MDr:n:s:W")) != EOF) {
        switch (c) {
        case 'd': dbname = optarg; break;
        case 'c': cdb2_set_comdb2db_config(optarg); break;
        case 'G':
            if (0 == strcasecmp(optarg, "partition")) {
                which_events |= PARTITION_EVENT;
            } else if (0 == strcasecmp(optarg, "sigstop")) {
                which_events |= SIGSTOP_EVENT;
            } else if (0 == strcasecmp(optarg, "clock")) {
                which_events |= CLOCK_EVENT;
            } else {
                fprintf(stderr, "Unknown test: %s\n", optarg);
                errors++;
            }
            break;
        case 't': cltype = optarg; break;
        case 'M': partition_master = 1; break;
        case 'W': partition_whole_network = 0; break;
        case 'D': debug_trace = 1; break;
        case 'r': runtime = atoi(optarg); break;
        case 'n': nemesis_length = atoi(optarg); break;
        case 's': sleep_time = atoi(optarg); break;
        default:
            fprintf(stderr, "Unknown option, '%c'\n", c);
            errors++;
            break;
        }
    }

    if (errors) {
        fprintf(stderr, "there were errors, exiting\n");
        usage(stdout);
        exit(1);
    }

    if (!dbname) {
        fprintf(stderr, "dbname is not specified\n");
        usage(stdout);
        exit(1);
    }

    if (which_events == 0) {
        fprintf(stderr, "NO NEMESIS SPECIFIED .. THIS SHOULD BE AN EASY RUN..\n");
    }

    srand(time(NULL) ^ getpid());

    if (partition_master) flags |= NEMESIS_PARTITION_MASTER;
    if (partition_whole_network) flags |= NEMESIS_PARTITION_WHOLE_NETWORK;
    if (debug_trace) flags |= NEMESIS_VERBOSE;
    for (cnt = 0; cnt < 1000 && n == NULL; cnt++) {
        n = nemesis_open(dbname, cltype, flags);
        if (!n) sleep(1);
    }

    if (!n) {
        fprintf(stderr, "Could not open nemesis for %s, exiting\n", dbname);
        myexit(__func__, __LINE__, 1);
    }

    fixall(n);
    printf("Fixed all\n");
    sleep(5);

    start_time = time(NULL);

    while(!runtime || ((time(NULL) - start_time) > runtime)) {
        if (which_events & PARTITION_EVENT) {
            breaknet(n);
        }
        if (which_events & SIGSTOP_EVENT) {
            signaldb(n, SIGSTOP, 0);
        }
        if (which_events & CLOCK_EVENT) {
            breakclocks(n, max_clock_skew);
        }

        if (nemesis_length > 0) sleep(nemesis_length);

        if (which_events & PARTITION_EVENT) {
            fixnet(n);
        }
        if (which_events & SIGSTOP_EVENT) {
            signaldb(n, SIGCONT, 1);
        }
        if (which_events & CLOCK_EVENT) {
            fixclocks(n);
        }
        if (sleep_time > 0) sleep(sleep_time);
    }

    printf("done\n");

    return 0;
}
