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

#include "locks.h"
#include "net.h"
#include "net_int.h"
#include "util.h"

struct timebuf {
    struct tm tm;
    char s[15];
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

static void basic_node_data(struct host_node_tag *ptr, FILE *out)
{
    struct timebuf t;
    char ip[16];
    fprintf(out, "host %10s%s:%-5d fd %-3d", ptr->host, ptr->subnet, ptr->port,
            ptr->fd);
    if (ptr->have_connect_thread)
        fprintf(out, " cnt_thd");
    if (ptr->have_reader_thread)
        fprintf(out, " rd_thd");
    if (ptr->have_writer_thread)
        fprintf(out, " wr_thd");
    if (ptr->decom_flag)
        fprintf(out, " decom");
    if (ptr->got_hello)
        fprintf(out, " hello");
    if (ptr->running_user_func)
        fprintf(out, " userfunc");
    if (ptr->closed)
        fprintf(out, " closed");
    if (ptr->really_closed)
        fprintf(out, " really_closed");
    if (ptr->distress)
        fprintf(out, " DISTRESS!");
    fprintf(out, "\n");

    fprintf(out, "  enque count %-5u peak %-5u at %s (hit max %u times)\n",
            ptr->enque_count, ptr->peak_enque_count,
            fmt_time(&t, ptr->peak_enque_count_time), ptr->num_queue_full);

    fprintf(out, "  enque bytes %-5u peak %-5u at %s\n", ptr->enque_bytes,
            ptr->peak_enque_bytes, fmt_time(&t, ptr->peak_enque_bytes_time));
}

static void basic_stat(netinfo_type *netinfo_ptr, FILE *out)
{
    struct host_node_tag *ptr;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        basic_node_data(ptr, out);
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

static void dump_node(netinfo_type *netinfo_ptr, FILE *out, char *host)
{
    struct host_node_tag *ptr;
    write_data *write_list_ptr;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        if (strcmp(ptr->host, host) == 0) {
            fprintf(out, "== NODE DUMP FOR %s ==\n", ptr->host);
            basic_node_data(ptr, out);
            fprintf(out, "dedupe_count: %u\n", ptr->dedupe_count);
            Pthread_mutex_lock(&(ptr->enquelk));
            fprintf(out, "write list %u items %u bytes:\n", ptr->enque_count,
                    ptr->enque_bytes);
            for (write_list_ptr = ptr->write_head; write_list_ptr != NULL;
                 write_list_ptr = write_list_ptr->next) {
                fprintf(out, "  typ %d age %2d flg %2x len %4u\n",
                        write_list_ptr->payload.header.type,
                        comdb2_time_epoch() - write_list_ptr->enque_time,
                        write_list_ptr->flags, (unsigned)write_list_ptr->len);
            }
            Pthread_mutex_unlock(&(ptr->enquelk));
        }
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

void net_cmd(netinfo_type *netinfo_ptr, char *line, int lline, int st, int op1)
{
    char *tok;
    int ltok;

    FILE *out = stdout;

    static const char *help_msg[] = {"stat    - basic stats",
                                     "dump #  - detailed dump of node #",
                                     "help    - help menu", NULL};

    tok = segtok(line, lline, &st, &ltok);
    if (ltok == 0 || tokcmp(tok, ltok, "stat") == 0) {
        basic_stat(netinfo_ptr, out);
    } else if (tokcmp(tok, ltok, "dump") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok) {
            char *host = tokdup(tok, ltok);
            dump_node(netinfo_ptr, out, host);
            free(host);
        }
    } else if (tokcmp(tok, ltok, "help") == 0) {
        int ii;
        for (ii = 0; help_msg[ii]; ii++)
            fprintf(out, "%s\n", help_msg[ii]);
    }
}
