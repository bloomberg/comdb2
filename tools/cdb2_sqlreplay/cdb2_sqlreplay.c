/*
   Copyright 2017 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/*
 * Comdb2 sql tool to replay sql logs
 *
 * $Id: cdb2_sqlreplay.c 88379 2017-03-27 20:11:12Z jmleddy $
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "cdb2api.h"
#include "cdb2_constants.h"

static cdb2_hndl_tp *cdb2h = NULL;

static const char *usage_text = 
    "Usage: cdb2sqlreplay dbname [FILE]\n" \
    "\n" \
    "Basic options:\n" \
    "-f                 run the sql as fast as possible\n";

/* Start of functions */
void usage() {
    printf(usage_text);
    exit(EXIT_FAILURE);
}

int timeval_subtract (struct timeval *result,
                      struct timeval *x,
                      struct timeval *y) {
    /* Perform the carry for the later subtraction by updating Y. */
    if (x->tv_usec < y->tv_usec) {
        int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
        y->tv_usec -= 1000000 * nsec;
        y->tv_sec += nsec;
    }
    if (x->tv_usec - y->tv_usec > 1000000) {
        int nsec = (x->tv_usec - y->tv_usec) / 1000000;
        y->tv_usec += 1000000 * nsec;
        y->tv_sec -= nsec;
    }

    /* Compute the time remaining to wait.
       'tv_usec' is certainly positive. */
    result->tv_sec = x->tv_sec - y->tv_sec;
    result->tv_usec = x->tv_usec - y->tv_usec;

    /* Return 1 if result is negative. */
    return x->tv_sec < y->tv_sec;
}

int tool_cdb2_sql_replay_main(int argc, char **argv) {
    FILE *infile;
    size_t line_alloc;
    char *line;
    char *post_prefix;
    char *stmt;
    struct tm stmt_tm;
    struct tm init_tm;
    struct timeval prgm_start;
    struct timeval now;
    struct timeval diff;
    int stmt_no;
    int wait_sec;
    int lineone;

    if (argc < 2) {
        usage();
    }
    if (argc >= 3) {
        if ((infile = fopen(argv[2], "r")) == NULL) {
            perror("Can't open input file");
            exit(EXIT_FAILURE);
        }
    } else {
        infile = stdin;
    }
    if (cdb2_open(&cdb2h, argv[1], "local", 0)) {
        fprintf(stderr,
                "cdb2_open() failed %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        exit(EXIT_FAILURE);
    }

    gettimeofday(&prgm_start, NULL);

    line = NULL;
    line_alloc = 0;
    lineone = 1;
    while ((getline(&line, &line_alloc, infile)) != -1) {
        memset(&stmt_tm, 0, sizeof(stmt_tm));
        if ((post_prefix = strptime(line, "%m/%d %T: ", &stmt_tm)) == NULL) {
            fprintf(stderr, "Input is not in sqlreqs format\n");
            exit(EXIT_FAILURE);
        }
        if (lineone) {
            memcpy(&init_tm, &stmt_tm, sizeof(stmt_tm));
            lineone = 0;
        }

        /* Only use sql_begin statments */
        if (strncmp(post_prefix, "sql_begin #", 11) != 0)
            continue;

        /* Advance to statement number */
        post_prefix+=11;
        stmt_no = strtol(post_prefix, &stmt, 0);

        /* Skip ':' */
        stmt++;
        /* Get to the good part */
        while (strncmp(stmt, "sql=", 4) != 0)
            stmt++;
        stmt+=4;

        /* We have our statement, wait until we need to use it */
        gettimeofday(&now, NULL);
        timeval_subtract(&diff, &now, &prgm_start);
        wait_sec = difftime(timelocal(&stmt_tm), timelocal(&init_tm));
        wait_sec -= diff.tv_sec;
        if (wait_sec > 0)
            sleep(wait_sec);

        cdb2_run_statement(cdb2h, stmt);
    }
    return 0;
}
