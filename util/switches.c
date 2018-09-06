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

/*
 * Varmon is accumulating lots of on/off switches.  Trying to tie them all
 * down in one module.
 * Stolen this module for comdb2, which has *even more* switches.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <segstr.h>

#include "switches.h"
#include "logmsg.h"

struct switch_type {
    const char *name;
    const char *descr;
    switch_on_fn on_fn;
    switch_off_fn off_fn;
    switch_stat_fn stat_fn;
    void *context;
};

static struct switch_type *switches = NULL;
static int num_switches = 0;
int maxnamelen = 0;

void register_switch(const char *name, const char *descr, switch_on_fn on_fn,
                     switch_off_fn off_fn, switch_stat_fn stat_fn,
                     void *context)
{
    struct switch_type *p;
    int len;

    p = realloc(switches, sizeof(struct switch_type) * (num_switches + 1));
    if (!p) {
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        return;
    }
    switches = p;

    switches[num_switches].name = strdup(name);
    switches[num_switches].descr = strdup(descr);
    switches[num_switches].on_fn = on_fn;
    switches[num_switches].off_fn = off_fn;
    switches[num_switches].stat_fn = stat_fn;
    switches[num_switches].context = context;

    if (!switches[num_switches].name || !switches[num_switches].descr) {
        logmsg(LOGMSG_ERROR, "%s: out of memory\n", __func__);
        return;
    }

    num_switches++;
    len = strlen(name);
    if (len > maxnamelen) {
        maxnamelen = len;
    }

    REGISTER_TUNABLE((char *)name, (char *)descr, TUNABLE_BOOLEAN, context,
                     NOARG, NULL, NULL, NULL, NULL);
}

void cleanup_switches()
{
    if (!switches) return;

    int ii;
    for (ii = 0; ii < num_switches; ii++) {
        if (switches[ii].name) { // populated via strdup
            free((void *)switches[ii].name);
            switches[ii].name = NULL;
        }
        if (switches[ii].descr) { // populated via strdup
            free((void *)switches[ii].descr);
            switches[ii].descr = NULL;
        }
    }
    free(switches);
}

void int_on_fn(void *context)
{
    *((int *)context) = 1;
}

void int_off_fn(void *context)
{
    *((int *)context) = 0;
}

int int_stat_fn(void *context)
{
    return *((const int *)context);
}

void register_int_switch(const char *name, const char *descr, int *flag)
{
    register_switch(name, descr, int_on_fn, int_off_fn, int_stat_fn, flag);
}

void switch_status(void)
{
    int ii;

    for (ii = 0; ii < num_switches; ii++) {
        logmsg(LOGMSG_USER, "%*s  %3s  (%s)\n", maxnamelen, switches[ii].name,
               switches[ii].stat_fn(switches[ii].context) ? "ON" : "off",
               switches[ii].descr);
    }
}

void change_switch(int on, char *line, int lline, int st)
{
    int ltok;
    char *tok;

    int ii;

    tok = segtok(line, lline, &st, &ltok);
    for (ii = 0; ii < num_switches; ii++) {
        if (tokcmp(tok, ltok, switches[ii].name) == 0) {
            if (on) {
                switches[ii].on_fn(switches[ii].context);
            } else {
                switches[ii].off_fn(switches[ii].context);
            }
            logmsg(LOGMSG_USER, "%s  %3s  (%s)\n", switches[ii].name,
                   switches[ii].stat_fn(switches[ii].context) ? "ON" : "off",
                   switches[ii].descr);
            return;
        }
    }

    logmsg(LOGMSG_ERROR, "Unrecognised switch %*.*s\n", ltok, ltok, tok);
}
