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

#define ANSI_COLOR_RED     "\x1b[31;1m"
#define ANSI_COLOR_YELLOW  "\x1b[33;1m"
#define ANSI_COLOR_RESET   "\x1b[0m"

int runtime = 0;
int max_clock_skew = 60;

char *dbname = NULL;
char *cltype = "dev";
char *argv0 = NULL;

uint32_t which_events = 0;
int partition_master = 0;
int partition_whole_network = 1;
int max_retries = 1000000;
int debug_trace = 0;
int nemesis_length = 10;
int sleep_time = 5;
int colored_output = 0;
int exit_on_block = 0;
int unblock_report_threshold = 0;
static int maxmon = 0;
static int curmon = 0;
static char **monfile = NULL;
static FILE **monfps = NULL;
static char **montext = NULL;

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
    fprintf(f, "        -C                  - use colored output\n");
    fprintf(f, "        -e                  - exit after block\n");
    fprintf(f, "        -u <threshold>      - set unblock-report threshold\n");
    fprintf(f, "        -m <file>           - add <file> to monitored files\n");
    fprintf(f, "        -r <runtime>        - set runtime in seconds\n");
    fprintf(f, "        -n <nemesis-length> - length of nemesis event\n");
    fprintf(f, "        -s <sleep-time>     - sleep between nemesis\n");
}

static void read_blocked_files(void)
{
    for (int i = 0 ; i < curmon; i++) {
        if (monfps[i] == NULL) {
            monfps[i] = fopen(monfile[i], "r");
        }
        else {
            rewind(monfps[i]);
        }
        if (monfps[i]) {
            char chk[1024] = {0};
            rewind(monfps[i]);
            fread(chk, sizeof(chk), 1, monfps[i]);
            if (montext[i]) 
                free(montext[i]);
            montext[i] = strdup(chk);
        }
    }
}

static int block_on_monitored_files(void)
{
    int blocked = 0;
    time_t start, end;
    static time_t longest_start;
    static time_t longest_end;
    static time_t longest = 0;
    static unsigned long long tot_wait_time = 0;
    static unsigned long long tot_wait_count = 0;

    start = time(NULL);
    for (int i = 0 ; i < curmon; i++) {
        if (monfps[i] == NULL) {
            monfps[i] = fopen(monfile[i], "r");
        }
        if (monfps[i]) {
            char chk[1024] = {0};
            int waited = 0;
            rewind(monfps[i]);
            fread(chk, sizeof(chk), 1, monfps[i]);
            while (montext[i] && !strcmp(montext[i], chk)) {
                if (colored_output) 
                    fprintf(stderr, ANSI_COLOR_RED);
                fprintf(stderr, "Waiting for monfile '%s' to change",
                        monfile[i]);
                if (colored_output) 
                    fprintf(stderr, ANSI_COLOR_RESET);
                fprintf(stderr, "\n");
                blocked = waited = 1;
                sleep (1);
                rewind(monfps[i]);
                fread(chk, sizeof(chk), 1, monfps[i]);
            }
            if (waited) {
                fprintf(stderr, "Monfile '%s' updated\n", monfile[i]);
            }
            if (montext[i]) 
                free(montext[i]);
            montext[i] = strdup(chk);
        }
        else {
            fprintf(stderr, "Skipping unopened file '%s'\n", monfile[i]);
        }
    }
    end = time(NULL);
    tot_wait_time += (end - start);
    tot_wait_count++;
    if (end - start > longest) {
        longest = end - start;
        longest_start = start;
        longest_end = end;
    }
    if ((end - start) > unblock_report_threshold) {
        struct tm start_tm, end_tm;
        localtime_r(&longest_start, &start_tm);
        localtime_r(&longest_end, &end_tm);
        if (colored_output)
            fprintf(stderr, ANSI_COLOR_YELLOW);
        fprintf(stderr, "Unblocked after %ld seconds", end - start);
        if (colored_output)
            fprintf(stderr, ANSI_COLOR_RESET);
        fprintf(stderr, "\n");
        if (colored_output)
            fprintf(stderr, ANSI_COLOR_YELLOW);
        fprintf(stderr, "Called %llu times, average wait time is %llu, longest was %ld seconds on "
                "%d/%d starting at %.2d:%.2d:%.2d and ending at %.2d:%.2d:%.2d",
                tot_wait_count, (unsigned long long)(tot_wait_time / tot_wait_count), longest, 
                start_tm.tm_mon+1, start_tm.tm_mday, start_tm.tm_hour, 
                start_tm.tm_min, start_tm.tm_sec, end_tm.tm_hour, end_tm.tm_min, 
                end_tm.tm_sec);
        if (colored_output)
            fprintf(stderr, ANSI_COLOR_RESET);
        fprintf(stderr, "\n");
    }

    return blocked;
}

int main(int argc, char *argv[])
{
    int c, errors = 0, cnt;
    uint32_t flags = 0, start_time;
    struct nemesis *n = NULL;

    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    argv0 = argv[0];

    while ((c = getopt(argc, argv, "d:c:G:t:MDr:n:s:Wm:Ceu:")) != EOF) {
        switch (c) {
        case 'd': dbname = optarg; break;
        case 'c': cdb2_set_comdb2db_config(optarg); break;
        case 'C': colored_output = 1; break;
        case 'e': exit_on_block = 1; break;
        case 'u': unblock_report_threshold = atoi(optarg); break;
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
        case 'm':
            if (curmon == maxmon) {
                maxmon = (maxmon ? maxmon << 1 : 10);
                monfile = realloc(monfile, sizeof(char *) * maxmon);
                montext = realloc(montext, sizeof(char *) * maxmon);
                monfps = realloc(monfps, sizeof(FILE *) * maxmon);
            }
            monfile[curmon++] = strdup(optarg);
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
    if (colored_output) flags |= NEMESIS_COLOR_PRINT;
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

        if (curmon)
            read_blocked_files();

        if (which_events & PARTITION_EVENT) {
            fixnet(n);
        }
        if (which_events & SIGSTOP_EVENT) {
            signaldb(n, SIGCONT, 1);
        }
        if (which_events & CLOCK_EVENT) {
            fixclocks(n);
        }

        int blocked = 0;

        sleep(1);

        if (curmon) {
            blocked |= block_on_monitored_files();
        }
        if (sleep_time > 0) 
            sleep(sleep_time);

        if (blocked && exit_on_block) {
            exit(1);
        }
    }

    printf("done\n");

    return 0;
}
