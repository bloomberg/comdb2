/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <dlfcn.h>

#include "comdb2.h"
#include "rtcpu.h"
#include <segstring.h>

static void usage(void)
{
    printf("'plugin' usage:\n");
    printf("  rtcpu /path/to/lib.so\n");
}

int load_rtcpu_plugin(struct dbenv *dbenv, char *lib_file)
{
    void *lib;
    int (*init)(void);
    int (*isup)(const char *);
    int (*mach_class)(const char *);

    lib = dlopen(lib_file, RTLD_LAZY);
    if (lib == NULL) {
        fprintf(stderr, "%s: %s\n", __func__, dlerror());
        return 1;
    }

    init = dlsym(lib, "init");
    if (init == NULL) {
        fprintf(stderr, "init %s\n", dlerror());
        return 1;
    }
    isup = dlsym(lib, "isup");
    if (isup == NULL) {
        fprintf(stderr, "isup %s\n", dlerror());
        return 1;
    }
    mach_class = dlsym(lib, "mach_class");
    if (mach_class == NULL) {
        fprintf(stderr, "mach_class %s\n", dlerror());
        return 1;
    }

    register_rtcpu_callbacks(isup, init, mach_class);
    return 0;
}

int process_plugin_command(struct dbenv *dbenv, char *line, int llen, int st,
                           int ltok)
{
    char *tok;
    char *lib;

    tok = segtok(line, llen, &st, &ltok);
    if (ltok == 0) {
        usage();
        return 0;
    }

    if (tokcmp(tok, ltok, "rtcpu") == 0) {
        tok = segtok(line, llen, &st, &ltok);
        if (ltok == 0) {
            usage();
            return 0;
        }
        lib = tokdup(tok, ltok);
        return load_rtcpu_plugin(dbenv, lib);
    } else {
        fprintf(stderr, "Unknown plugin type\n");
        return 1;
    }
    return 0;
}
