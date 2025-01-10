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

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include <segstr.h>
#include <stdarg.h>
#include <compat.h>

#include "comdb2.h"
#include "tag.h"
#include "logmsg.h"
#include "time_accounting.h"
#include "intern_strings.h"

extern int gbl_debug_recover_deadlock_evbuffer;
/* print a description of each tcm test */
static void tcmtest_printlist()
{
    logmsg(LOGMSG_USER, "routecpu <node>            - set routecpu test-node to <node> (routes off the node)\n");
    logmsg(LOGMSG_USER, "\n");
}

char *tcmtest_routecpu_down_node;

/* parse debug trap @send debug <cmd> */
void debug_trap(char *line, int lline)
{
    char table[MAXTABLELEN];
    char tag[MAXTAGLEN];
    int st = 0;
    char *tok;
    int ltok;

    tok = segtok(line, lline, &st, &ltok);
    if (tokcmp(tok, ltok, "delsc") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok >= MAXTABLELEN || ltok <= 0) {
           logmsg(LOGMSG_ERROR, "Invalid table.\n");
            return;
        }
        tokcpy(tok, ltok, table);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok >= MAXTABLELEN || ltok <= 0) {
           logmsg(LOGMSG_ERROR, "Invalid tag.\n");
            return;
        }
        tokcpy(tok, ltok, tag);
        del_tag_schema(table, tag);
    } else if (tokcmp(tok, ltok, "tcmtest") == 0) {

        /* grab the tcmtest-name */
        tok = segtok(line, lline, &st, &ltok);

        /* list tcmtests */
        if ((tokcmp(tok, ltok, "list") == 0) ||
            (tokcmp(tok, ltok, "help") == 0)) {
            tcmtest_printlist();
        }

        else if (tokcmp(tok, ltok, "routecpu") == 0) {
            tok = segtok(line, lline, &st, &ltok);
            char *host = NULL;
            if (ltok > 0) {
                char *tmphost = tokdup(tok, ltok);
                char *end = NULL;
                int node = strtol(tmphost, &end, 10);
                if (*end == 0) { /* consumed entire token */
                    if (node > 0) {
                        host = hostname(node);
                    }
                } else {
                    host = intern(tmphost);
                }
                free(tmphost);
            }
            logmsg(LOGMSG_USER, "%s routecpu test for node %s\n",
                   host ? "enable" : "disable",
                   host ? host : tcmtest_routecpu_down_node);
            tcmtest_routecpu_down_node = host;
        }
    } else if (tokcmp(tok, ltok, "timings") == 0) {
        print_all_time_accounting();
    } else if (tokcmp(tok, ltok, "recover_deadlock_evbuffer") == 0) {
        printf("%s recover_deadlock_evbuffer\n", __func__);
        gbl_debug_recover_deadlock_evbuffer = 1;
    } else if (tokcmp(tok, ltok, "help") == 0) {
        logmsg(LOGMSG_USER, "tcmtest <test>       - enable a cdb2tcm test\n");
        logmsg(LOGMSG_USER, "tcmtest list         - list cdb2tcm tests\n");
        logmsg(LOGMSG_USER, "getvers table        - get schema version for table (or all)\n");
        logmsg(LOGMSG_USER, "putvers table num    - set schema version for table\n");
        logmsg(LOGMSG_USER, "delsc   table tag    - delete a tag\n");
        logmsg(LOGMSG_USER, "timings              - print all accumulated "
                            "timing measurements \n");
    } else {
        logmsg(LOGMSG_ERROR, "Unknown debug command <%.*s>\n", ltok, tok);
    }
}
