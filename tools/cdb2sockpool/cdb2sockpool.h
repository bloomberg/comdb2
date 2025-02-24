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

#include <list.h>
#include <tcputil.h>

#ifndef INC__SQLPROXY_H
#define INC__SQLPROXY_H

#include <plhash_glue.h>
#include <pool.h>

#define BOOL_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, BOOL)
#define VALUE_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, VALUE)
#define SECS_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, SECS)
#define MSECS_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, MSECS)
#define BYTES_SETTING(var, dfl, descr) SETTING_MACRO(var, dfl, descr, BYTES)

/* Declare all global settings */
struct setting;
#define SETTING_MACRO(var, dfl, descr, type) extern unsigned var;
#include "settings.h"
#undef SETTING_MACRO

enum { MAX_BUFLINKS = 10 };

/* Used to describe a segment of one of our send or receive buffers. */
struct buflink {
    LINKC_T(struct buflink) linkv;
    unsigned offset;
    unsigned length;
    int ignore;
};

extern pthread_mutex_t sockpool_lk;

void print_setting(const struct setting *setting);
void print_all_settings(void);
int set_setting(const char *name, unsigned new_value);
void setting_changed(unsigned *setting);

void *local_accept_thd(void *voidarg);

#endif /* INC__SQLPROXY_H */
