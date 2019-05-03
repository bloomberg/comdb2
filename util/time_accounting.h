/*
   Copyright 2019 Bloomberg Finance L.P.

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

#ifndef _time_accounting_h
#define _time_accounting_h

#include <sys/time.h>

#ifndef NDEBUG
enum { CHR_IXADDK, CHR_DATADD, CHR_TMPSVOP, CHR_MAX };

#define ACCUMULATE_TIMING(NAME, CODE) do { \
    struct timeval __tv; \
    gettimeofday(&__tv, NULL); \
    CODE; \
    accumulate_time(NAME, chrono_stop(&__tv)); \
} while(0); 

int chrono_stop(struct timeval *tv);
void accumulate_time(int el, int us);

void print_time_accounting(int el);
void print_all_time_accounting();

void reset_time_accounting(int el);
void reset_all_time_accounting();

#else
#define ACCUMULATE_TIMING(NAME, CODE) do { CODE; } while(0);
#define print_all_time_accounting() {}
#define cleanup_time_accounting() {}
#endif


#endif
