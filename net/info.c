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

/*
 * Message trap interface to query net status.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <epochlib.h>
#include <segstr.h>
#include <str0.h>

#include "sys_wrap.h"
#include "net.h"
#include "net_int.h"
#include "util.h"

#include "mem_net.h"
#include "mem_override.h"

struct timebuf {
    struct tm tm;
    char s[64];
};

static char *fmt_time(struct timebuf *b, int epochtime)
{
    time_t timet = (time_t)epochtime;
    if (epochtime == 0) {
        strncpy0(b->s, "--/-- --:--:--", sizeof(b->s));
    } else {
        localtime_r(&timet, &b->tm);
        snprintf(b->s, sizeof(b->s), "%02d/%02d %02d:%02d:%02d",
                 b->tm.tm_mon + 1, b->tm.tm_mday, b->tm.tm_hour, b->tm.tm_min,
                 b->tm.tm_sec);
    }
    return b->s;
}

static void basic_stat(netinfo_type *netinfo_ptr)
{
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    net_subnet_status();

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

void net_cmd(netinfo_type *netinfo_ptr, char *line, int lline, int st, int op1)
{
    char *tok;
    int ltok;

    static const char *help_msg[] = {"stat    - basic stats",
                                     "help    - help menu", NULL,
                                     "conn     - dump connection stats"};

    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0 || tokcmp(tok, ltok, "stat") == 0) {
        basic_stat(netinfo_ptr);
    } else if (tokcmp(tok, ltok, "dump") == 0) {
        tok = segtok(line, lline, &st, &ltok);
    } else if (tokcmp(tok, ltok, "help") == 0) {
        int ii;
        for (ii = 0; help_msg[ii]; ii++)
            logmsg(LOGMSG_USER, "%s\n", help_msg[ii]);
    }
    else if (tokcmp(tok, ltok, "conn") == 0)
    {
        logmsg(LOGMSG_USER, "All connect times\n");
        quantize_dump(netinfo_ptr->conntime_all, stdout);
        logmsg(LOGMSG_USER, "\n");
        logmsg(LOGMSG_USER, "Connect times in last %d seconds\n", netinfo_ptr->conntime_dump_period);
        quantize_dump(netinfo_ptr->conntime_periodic, stdout);
    }
}

int64_t net_get_num_accepts(netinfo_type *netinfo_ptr) {
   return netinfo_ptr->num_accepts;
}

int64_t net_get_num_accept_timeouts(netinfo_type *netinfo_ptr) {
   return netinfo_ptr->num_accept_timeouts;
}

int64_t net_get_num_current_non_appsock_accepts(netinfo_type *netinfo_ptr) {
   return netinfo_ptr->num_current_non_appsock_accepts;
}
