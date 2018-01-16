/*
 * Copyright (c) 1987, 1993, 1994, 1996
 *  The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *  This product includes software developed by the University of
 * California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bb_getopt_long.h"

extern int opterr;   /* if error message should be printed */
extern int optind;   /* index into parent argv vector */
extern int optopt;   /* character checked for validity */
extern char *optarg; /* argument associated with option */

static int initialized = 0;

#define __MYP(x) x
#define _DIAGASSERT(x) assert(x)

static char *__progname __MYP((char *));
static int getopt_internal __MYP((int, char *const *, const char *));

static char *__progname(char *nargv0)
{
    char *tmp;

    _DIAGASSERT(nargv0 != NULL);

    tmp = strrchr(nargv0, '/');
    if (tmp)
        tmp++;
    else
        tmp = nargv0;
    return (tmp);
}

#define BADCH (int)'?'
#define BADARG (int)':'
#define EMSG ""

static void swapargs(char **argv, int left, int mid, int right)
{
    char *k;
    int ii;
    while (left < mid && mid < right) {
        if (mid - left < right - mid) {
            for (ii = 0; ii < mid - left; ii++) {
                k = argv[left + ii];
                argv[left + ii] = argv[right - (mid - left) + ii];
                argv[right - (mid - left) + ii] = k;
            }
            right -= (mid - left);
        } else {
            for (ii = 0; ii < right - mid; ii++) {
                k = argv[left + ii];
                argv[left + ii] = argv[mid + ii];
                argv[mid + ii] = k;
            }
            left += (right - mid);
        }
    }
}

static int opt_idx(char *opt, char *options, struct option *long_options,
                   int *req)
{
    char *p;
    int ii, len;

    if (opt[0] != '-') return -1;

    if ((++opt)[0] == '-')
        ++opt;
    else if (strlen(opt) == 1) {
        if ((p = strchr(options, opt[0])) != NULL) {
            if (req) {
                *req = p[1] == ':' ? required_argument : no_argument;
            }
            return -2;
        }
    }

    len = (p = strchr(opt, '=')) ? (int)(p - opt) : strlen(opt);

    for (ii = 0; long_options[ii].name; ii++) {
        if (!strncmp(opt, long_options[ii].name, len) &&
            strlen(long_options[ii].name) == len) {
            if (req) *req = long_options[ii].has_arg;
            return ii;
        }
    }

    return -1;
}

static void replace_args(int argc, char *argv[], char *options,
                         struct option *long_options)
{
    int ii, req_arg, idx, left, mid;

    for (ii = 1, left = 1, mid = 1; ii < argc; ii++) {
        if (left == mid) left = mid = ii;
        if (strcmp(argv[ii], "--") == 0) {
            if (left < mid && mid < ii + 1) {
                swapargs(argv, left, mid, ii + 1);
                left = mid = 0;
                break;
            }
        } else if ((idx = opt_idx(argv[ii], options, long_options, &req_arg)) !=
                   -1) {
            if (idx >= 0 && argv[ii][1] != '-') {
                char *r = (char *)malloc(strlen(argv[ii]) + 2);
                r[0] = '-';
                strcpy(&r[1], argv[ii]);
                argv[ii] = r;
            }
            if (req_arg == required_argument && strchr(argv[ii], '=') == NULL) {
                ii++;
            }
        } else {
            if (left < mid && mid < ii) {
                swapargs(argv, left, mid, ii);
                left = ii - (mid - left);
            }
            while ((mid = (ii + 1)) < argc && argv[mid][0] != '-')
                ii++;
        }
    }

    if (left < mid && mid < argc) swapargs(argv, left, mid, argc);
}

/*
 * getopt --
 *  Parse argc/argv argument vector.
 */
static int getopt_internal(int nargc, char *const *nargv, const char *ostr)
{
    static char *place = EMSG; /* option letter processing */
    char *oli;                 /* option letter list index */

    _DIAGASSERT(nargv != NULL);
    _DIAGASSERT(ostr != NULL);

    if (!*place) { /* update scanning pointer */
        if (optind >= nargc || *(place = nargv[optind]) != '-') {
            place = EMSG;
            return (-1);
        }
        if (place[1] && *++place == '-') { /* found "--" */
            /* ++optind; */
            place = EMSG;
            return (-2);
        }
    } /* option letter okay? */
    if ((optopt = (int)*place++) == (int)':' || !(oli = strchr(ostr, optopt))) {
        /*
         *       * if the user didn't specify '-' as an option,
         *               * assume it means -1.
         *                       */
        if (optopt == (int)'-') return (-1);
        if (!*place) ++optind;
        if (opterr && *ostr != ':')
            (void)fprintf(stderr, "%s: illegal option -- %c\n",
                          __progname(nargv[0]), optopt);
        return (BADCH);
    }
    if (*++oli != ':') { /* don't need argument */
        optarg = NULL;
        if (!*place) ++optind;
    } else {        /* need an argument */
        if (*place) /* no white space */
            optarg = place;
        else if (nargc <= ++optind) { /* no arg */
            place = EMSG;
            if ((opterr) && (*ostr != ':'))
                (void)fprintf(stderr, "%s: option requires an argument -- %c\n",
                              __progname(nargv[0]), optopt);
            return (BADARG);
        } else /* white space */
            optarg = nargv[optind];
        place = EMSG;
        ++optind;
    }
    return (optopt); /* dump back option letter */
}

/*
 * getopt_long --
 *  Parse argc/argv argument vector.
 */
int bb_getopt_long(int nargc, char **nargv, char *options,
                   struct option *long_options, int *index)
{
    int retval;

    _DIAGASSERT(nargv != NULL);
    _DIAGASSERT(options != NULL);
    _DIAGASSERT(long_options != NULL);
    /* index may be NULL */
    optopt = '\0';

    if (initialized == 0 || optind == 0) {
        replace_args(nargc, nargv, options, long_options);
        optind = initialized = 1;
    }

    if ((retval = getopt_internal(nargc, nargv, options)) == -2) {
        char *current_argv = nargv[optind++] + 2, *has_equal;
        int i, current_argv_len, match = -1;

        if (*current_argv == '\0') {
            return (-1);
        }
        if ((has_equal = strchr(current_argv, '=')) != NULL) {
            current_argv_len = has_equal - current_argv;
            has_equal++;
        } else
            current_argv_len = strlen(current_argv);

        for (i = 0; long_options[i].name; i++) {
            if (strncmp(current_argv, long_options[i].name, current_argv_len))
                continue;

            if (strlen(long_options[i].name) == (unsigned)current_argv_len) {
                match = i;
                break;
            }
            if (match == -1) match = i;
        }
        if (match != -1) {
            if (long_options[match].has_arg == required_argument ||
                long_options[match].has_arg == optional_argument) {
                if (has_equal)
                    optarg = has_equal;
                else if (long_options[match].has_arg == optional_argument)
                    optarg = NULL;
                else
                    optarg = nargv[optind++];
            } else {
                optarg = NULL;
            }

            if ((long_options[match].has_arg == required_argument) &&
                (optarg == NULL)) {
                /*
                 * Missing argument, leading :
                 * indicates no error should be generated
                 */
                if ((opterr) && (*options != ':'))
                    (void)fprintf(stderr,
                                  "%s: option requires an argument -- %s\n",
                                  __progname(nargv[0]), current_argv);
                return (BADCH);
            }
        } else { /* No matching argument */
            if ((opterr) && (*options != ':'))
                (void)fprintf(stderr, "%s: illegal option -- %s\n",
                              __progname(nargv[0]), current_argv);
            return (BADCH);
        }
        if (long_options[match].flag) {
            *long_options[match].flag = long_options[match].val;
            retval = 0;
        } else
            retval = long_options[match].val;
        if (index) *index = match;
    }
    return (retval);
}
