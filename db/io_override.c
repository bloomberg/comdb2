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

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>

/* Override what stdout/stderr mean for a thread.  This is useful to
   get mtrap text to go over a socket without changing every printf
   in the database*/

extern pthread_key_t iokey;

#ifdef NOINTERLEAVE
static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
#endif

void sqlite3DebugPrintf(const char *zFormat, ...)
{
    va_list args;
    FILE *override;

#ifdef NOINTERLEAVE
    pthread_mutex_lock(&lk);
#endif

    va_start(args, zFormat);
    override = pthread_getspecific(iokey);
    if (override == NULL)
        override = stdout;
    vfprintf(override, zFormat, args);
    va_end(args);

#ifdef NOINTERLEAVE
    pthread_mutex_unlock(&lk);
#endif
}

int fprintf(FILE *f, const char *fmt, ...)
{
    int rc;
    va_list args;
    FILE *override;

#ifdef NOINTERLEAVE
    pthread_mutex_lock(&lk);
#endif

    va_start(args, fmt);
    override = pthread_getspecific(iokey);
    if (override == NULL || (f != stdout && f != stderr))
        override = f;
    rc = vfprintf(override, fmt, args);
    va_end(args);

#ifdef NOINTERLEAVE
    pthread_mutex_unlock(&lk);
#endif

    return rc;
}

int printf(const char *fmt, ...)
{
    int rc;
    va_list args;
    FILE *override;

#ifdef NOINTERLEAVE
    pthread_mutex_lock(&lk);
#endif

    va_start(args, fmt);
    override = pthread_getspecific(iokey);
    if (override == NULL)
        override = stdout;
    rc = vfprintf(override, fmt, args);
    va_end(args);

#ifdef NOINTERLEAVE
    pthread_mutex_unlock(&lk);
#endif

    return rc;
}

//int io_override_init(void) { return pthread_key_create(&iokey, NULL); }

int io_override_set_std(FILE *f)
{
    pthread_setspecific(iokey, f);
    return 0;
}

FILE *io_override_get_std(void) { return pthread_getspecific(iokey); }
