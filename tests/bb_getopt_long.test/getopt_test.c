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
 * Tests for bb_getopt_long.c
 */

#if defined NORMAL_GETOPT
#include <getopt.h>
#else
#include <bb_getopt_long.h>
#endif

#include <stdlib.h>
#include <stdio.h>

int reqarg0;
int reqarg1;
int reqarg2;
int noarg3;
int noarg4;
int optarg5;
int optarg6;

static struct option long_options[] = {
    {"rarg0", required_argument, &reqarg0, 0},
    {"rarg1", required_argument, &reqarg1, 1},
    {"rarg2", required_argument, &reqarg2, 2},
    {"noarg3", no_argument, &noarg3, 3},
    {"noarg4", no_argument, &noarg4, 4},
    {"optarg5", optional_argument, &optarg5, 5},
    {"optarg6", optional_argument, &optarg6, 6},
    {"rarg7", required_argument, NULL, 7},
    {"rarg8", required_argument, NULL, 8},
    {"optarg9", optional_argument, NULL, 9},
    {"optarg10", optional_argument, NULL, 10},
    {"noarg11", no_argument, NULL, 11},
    {"noarg12", no_argument, NULL, 12},
    {"usage", no_argument, NULL, 'u'},
    {"print-command-line", no_argument, NULL, 'p'},
    {NULL, 0, NULL, 0}};

char *argtype_str(int argtype)
{
    switch (argtype) {
    case required_argument: return "required_argument"; break;
    case no_argument: return "no_argument"; break;
    case optional_argument: return "optional_argument"; break;
    default: return "(unsupported)"; break;
    }
}

static void print_long_opt(FILE *f, char c, int idx, char optopt, char *optarg,
                           char *varname, int *var)
{
    fprintf(f, "long_opt returns %x, idx %d, name='%s', type='%s', "
               "optopt='%c', optarg='%s', varname='%s', var=%d\n",
            c, idx, long_options[idx].name,
            argtype_str(long_options[idx].has_arg), optopt, optarg,
            varname ? varname : "(null)", var ? *var : -1);
}

static void print_char_opt(FILE *f, char c, char optopt, char *optarg)
{
    fprintf(f, "single-char-opt '%c' optarg '%s'\n", c, optarg);
}

static void dump_long_options(FILE *f)
{
    for (int ii = 0; long_options[ii].name != NULL; ii++) {
        fprintf(f, "%19s %17s %8p %d\n", long_options[ii].name,
                argtype_str(long_options[ii].has_arg), long_options[ii].flag,
                long_options[ii].val);
    }
}

static void print_usage(FILE *f, char *argv0)
{
    fprintf(f, "%s:\n", argv0);
    dump_long_options(f);
}

static void print_command_line(FILE *f, int argc, char *argv[])
{
    for (int ii = 1; ii < argc; ii++) {
        fprintf(f, "%s%c", argv[ii], ii == (argc - 1) ? '\n' : ' ');
    }
}

/* Our version of getopt will re-order the actual commandline options before
 * parsing.  This means that the command-line optind value will be different
 * after the first call to getopt_long.  A bigger difference is that we assume
 * a single dash followed by multiple characters is a longopt, rather than
 * several short-opts. */
int main(int argc, char *argv[])
{
    int rc = 0, usage = 0, pcommand = 0;
    int options_idx;
    int ret = 0;

#if defined NORMAL_GETOPT
    while ((ret = getopt_long(argc, argv, "ha:pu", long_options, &options_idx)) !=
           -1) {
#else
    while ((ret = bb_getopt_long(argc, argv, "ha:pu", long_options,
                               &options_idx)) != -1) {
#endif
        char c = ret;
        if (c == 'h') {
            print_char_opt(stdout, c, optopt, optarg);
            usage = 1;
            break;
        }
        if (c == 'a') {
            print_char_opt(stdout, c, optopt, optarg);
            continue;
        }
        if (c == 'p') {
            print_char_opt(stdout, c, optopt, optarg);
            pcommand = 1;
            continue;
        }
        if (c == 'u') {
            print_char_opt(stdout, c, optopt, optarg);
            usage = 1;
            continue;
        }
        if (c == '?') {
            print_char_opt(stdout, c, optopt, optarg);
            continue;
        }

        switch (options_idx) {
        case 0:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "reqarg0",
                           &reqarg0);
            break;
        case 1:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "reqarg1",
                           &reqarg1);
            break;
        case 2:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "reqarg2",
                           &reqarg2);
            break;
        case 3:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "noarg3",
                           &noarg3);
            break;
        case 4:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "noarg4",
                           &noarg4);
            break;
        case 5:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "optarg5",
                           &optarg5);
            break;
        case 6:
            print_long_opt(stdout, c, options_idx, optopt, optarg, "optarg6",
                           &optarg6);
            break;
        case 7:
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            break;
        case 8:
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            break;
        case 9:
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            break;
        case 10:
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            break;
        case 11:
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            break;
        case 12:
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            break;
        case 'u':
            print_long_opt(stdout, c, options_idx, optopt, optarg, NULL, NULL);
            usage = 1;
            break;
        default:
            fprintf(stderr, "Unknown option %d, optopt=%d\n", c, optopt);
            break;
        }
    }

    if (usage) {
        print_usage(stdout, argv[0]);
    }

    if (pcommand) {
        print_command_line(stdout, argc, argv);
    }

    return rc;
}
