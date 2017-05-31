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
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "cdb2api.h"
#include "cson_amalgamation_core.h"

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

int main(int argc, char **argv) {
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
    int linenum;
    int linelen;

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
    /* TODO: tier should be an option */
#if 0
    if (cdb2_open(&cdb2h, argv[1], "local", 0)) {
        fprintf(stderr,
                "cdb2_open() failed %s\n", cdb2_errstr(cdb2h));
        cdb2_close(cdb2h);
        exit(EXIT_FAILURE);
    }
#endif

    gettimeofday(&prgm_start, NULL);

    line = NULL;
    line_alloc = 0;
    linenum = 1;
    while ((linelen = getline(&line, &line_alloc, infile)) != -1) {
        int rc;
        cson_value *obj;
        cson_parse_info pinfo = cson_parse_info_empty_m;

        rc = cson_parse_string(&obj, line, linelen, &cson_parse_opt_empty, &pinfo);
        if (rc) {
            fprintf(stderr, "Malformed input on line %d\n", linenum);
        }

        memset(&stmt_tm, 0, sizeof(stmt_tm));

        if (linenum == 1)
            memcpy(&init_tm, &stmt_tm, sizeof(stmt_tm));

        /* We have our statement, wait until we need to use it */
        gettimeofday(&now, NULL);
        timeval_subtract(&diff, &now, &prgm_start);
        wait_sec = difftime(timelocal(&stmt_tm), timelocal(&init_tm));
        wait_sec -= diff.tv_sec;
        if (wait_sec > 0)
            sleep(wait_sec);

        // cdb2_run_statement(cdb2h, stmt);

        cson_free_value(obj);
        linenum++;
    }
    // cdb2_close(cdb2h);
    return 0;
}
