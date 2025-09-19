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

#ifndef INCLUDED_BBHRTIME_H
#define INCLUDED_BBHRTIME_H

#include <bb_stdint.h>

#include <stdint.h>
#include <time.h>
#include <sys/time.h>

#if defined __cplusplus
extern "C" {
#endif

typedef struct timespec bbhrtime_t;
typedef struct timeval bbtime_t;

/**
 * NOTES:
 *  - {wiki HighRes Time Benchmarks<go>} aims to document how expensive
 *    which calls are.
 *  - LINUX: gettimeofday gives us only microsecond granularity.
 **/

int getbbhrtime(bbhrtime_t *);
int getbbhrvtime(bbhrtime_t *);
/* bbhrtimens converts bbhrtime_t to nanoseconds, returns negative value on
 * error */
int64_t bbhrtimens(const bbhrtime_t *t);
/* bbhrtimeus converts bbhrtime_t to microseconds, returns negative value on
 * error */
double bbhrtimeus(const bbhrtime_t *t);
/* bbhrtimems converts bbhrtime_t to milliseconds, returns negative value on
 * error */
double bbhrtimems(const bbhrtime_t *t);
/* bbhrtime converts bbhrtime_t to seconds, returns negative value on error */
double bbhrtime(const bbhrtime_t *t);

bbint64_t diff_bbhrtime(bbhrtime_t *, bbhrtime_t *);

int getbbtime(bbtime_t *t);
bbint64_t diff_bbtime(bbtime_t *end, bbtime_t *start);

#if defined __cplusplus
}
#endif

#endif
