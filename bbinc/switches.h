/*
   Copyright 2015, 2017 Bloomberg Finance L.P.

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

#ifndef INC__SWITCHES_H
#define INC__SWITCHES_H

#include "tunables.h"

/* Called to turn a switch on.  The switch may already be on.  The
 * implementation doesn't have to turn the switch on (it may not be safe to
 * do so for example). */
typedef void (*switch_on_fn)(void *context);

/* Called to turn a switch off; switch may already be off; doesn't have to
 * if it doesn't want to. */
typedef void (*switch_off_fn)(void *context);

/* Should return 0 if switch is off, non-zero if switch is on. */
typedef int (*switch_stat_fn)(void *context);

void register_switch(const char *name, const char *descr, switch_on_fn on_fn,
                     switch_off_fn off_fn, switch_stat_fn stat_fn,
                     void *context);

// cleanup memory
void cleanup_switches();

/* register a switch that can be turned on and off at will and which has its
 * status recorded in a simple 32 bit integer as 0 (off) or 1 (on). */
void int_on_fn(void *context);
void int_off_fn(void *context);
int int_stat_fn(void *context);
void register_int_switch(const char *name, const char *descr, int *flag);

void switch_status(void);

void change_switch(int on, char *line, int lline, int st);

int init_debug_switches(void);

#endif /* INC__SWITCHES_H */
