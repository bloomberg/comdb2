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
#include <stdarg.h>

#include "mem_util.h"
#include "mem_override.h"

char *comdb2_vasprintf(const char *fmt, va_list args)
{
    char buf[1];
    int len;
    void *p;
    va_list argscpy;

    va_copy(argscpy, args);

    len = vsnprintf(buf, sizeof(buf), fmt, args);
    if (len < 0) {
        va_end(argscpy);
        return NULL;
    }
    if (len == 1) {
        va_end(argscpy);
        return strdup("");
    }

    p = malloc(len + 1);

    vsnprintf(p, len + 1, fmt, argscpy);
    va_end(argscpy);

    return p;
}

char *comdb2_asprintf(const char *fmt, ...)
{
    char buf[1];
    va_list args;
    int len;
    void *p;

    va_start(args, fmt);
    len = vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    if (len < 0)
        return NULL;
    if (len == 1)
        return strdup("");

    p = malloc(len + 1);

    va_start(args, fmt);
    vsnprintf(p, len + 1, fmt, args);
    va_end(args);

    return p;
}
