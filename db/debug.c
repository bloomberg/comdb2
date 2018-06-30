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

#include "comdb2.h"
#include "tag.h"
#include "net.h"

#include "nodemap.h"
#include "logmsg.h"

void tcmtest_routecpu_set_down_node(int n);
int osql_disable_net_test(void);
int osql_enable_net_test(int testnum);

/* which netinfo_ptr to use */
enum { NETINFO_NONE = 0, NETINFO_OSQL = 1, NETINFO_REP = 2 };

/* which event */
enum { NETTEST_NONE = 0, NETTEST_ENABLE = 1, NETTEST_DISABLE = 2 };

/* keep track of any tcm-test state that we need */
static int tcmtest_flag = 0;

/* check if a tcm test is enabled */
int tcm_testpoint(int tcmtestid)
{
    if (tcmtestid < 0 || tcmtestid > TCM_MAX) {
        logmsg(LOGMSG_ERROR, "invalid tcm testid %d\n", tcmtestid);
        return 0;
    }

    if (tcmtest_flag == tcmtestid) {
        return 1;
    }

    return 0;
}

/* print a description of each tcm test */
static void tcmtest_printlist()
{
    logmsg(LOGMSG_USER, "ptranfail <on|off>         - enable/disable toblock "
                    "parenttran deadlock test\n");
    logmsg(LOGMSG_USER, "routecpu <node>            - set routecpu test-node to "
                    "<node> (routes off the node)\n");
    logmsg(LOGMSG_USER, "nettest <opts>             - enable net debugging - use "
                    "'help' for opts\n");
    logmsg(LOGMSG_USER, "\n");
}

/* nettest usage */
static void nettest_usage(void)
{
    logmsg(LOGMSG_USER, "Usage: nettest [enable|disable] [rep|osql] <testname>\n");
    logmsg(LOGMSG_USER, "Only testname currently is 'queuefull' test\n");
    logmsg(LOGMSG_USER, "\n");
}

/* parse debug trap */
void debug_trap(char *line, int lline)
{
    char table[MAXTABLELEN];
    char tag[MAXTAGLEN];
    int i;
    int rc;
    int st = 0;
    char *tok;
    int ltok;
    int newvers;

    tok = segtok(line, lline, &st, &ltok);
    if (tokcmp(tok, ltok, "delsc") == 0) {
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > MAXTABLELEN || ltok <= 0) {
           logmsg(LOGMSG_ERROR, "Invalid table.\n");
            return;
        }
        tokcpy(tok, ltok, table);
        tok = segtok(line, lline, &st, &ltok);
        if (ltok > MAXTABLELEN || ltok <= 0) {
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
            if (ltok <= 0) {
                logmsg(LOGMSG_ERROR, 
                        "routecpu command requires a node to route-off argument\n");
            } else {
                int node = toknum(tok, ltok);
                if (node > 0) {
                    tcmtest_routecpu_set_down_node(node);
                    logmsg(LOGMSG_USER, "enabled routecpu test for node %d\n", node);
                } else {
                    tcmtest_routecpu_set_down_node(0);
                    logmsg(LOGMSG_USER, "disabled routecpu test\n");
                }
            }
        }
        /* netdebug tests */
        else if (tokcmp(tok, ltok, "nettest") == 0) {
            int whichnet = NETINFO_NONE;
            int whichtest = NET_TEST_NONE;
            int whichevt = NETTEST_NONE;

            /* 'enable' or 'disable' */
            tok = segtok(line, lline, &st, &ltok);
            if (ltok <= 0 || tokcmp(tok, ltok, "help") == 0) {
                nettest_usage();
                return;
            } else if (tokcmp(tok, ltok, "enable") == 0) {
                whichevt = NETTEST_ENABLE;
            } else if (tokcmp(tok, ltok, "disable") == 0) {
                whichevt = NETTEST_DISABLE;
            } else {
                logmsg(LOGMSG_ERROR, "expected 'enable' or 'disable'\n");
                nettest_usage();
                return;
            }

            /* 'rep' or 'osql' */
            tok = segtok(line, lline, &st, &ltok);
            if (ltok <= 0) {
               logmsg(LOGMSG_ERROR, "expected 'rep' or 'osql' network layer\n");
                nettest_usage();
                return;
            } else if (tokcmp(tok, ltok, "rep") == 0) {
                whichnet = NETINFO_REP;
            } else if (tokcmp(tok, ltok, "osql") == 0) {
                whichnet = NETINFO_OSQL;
            }

            if (NETINFO_NONE == whichnet) {
               logmsg(LOGMSG_ERROR, "invalid netinfo target for nettest\n");
                nettest_usage();
                return;
            }

            /* 'queuefull' */
            tok = segtok(line, lline, &st, &ltok);
            if (ltok > 0 && tokcmp(tok, ltok, "queuefull") == 0) {
                whichtest = NET_TEST_QUEUE_FULL;
            } else if (NETTEST_ENABLE == whichevt) {
               logmsg(LOGMSG_ERROR, "invalid test\n");
                nettest_usage();
                return;
            }

            /* disable test*/
            if (whichevt == NETTEST_DISABLE) {
                if (NETINFO_OSQL == whichnet) {
                    osql_disable_net_test();
                } else {
                    net_disable_test(thedb->handle_sibling);
                }
            }
            /* enable test */
            else {
                if (NETINFO_OSQL == whichnet) {
                    osql_enable_net_test(whichtest);
                } else {
                    net_enable_test(thedb->handle_sibling, whichtest);
                }
            }
        }

        /* ptranfail test */
        else if (tokcmp(tok, ltok, "ptranfail") == 0) {
            tok = segtok(line, lline, &st, &ltok);

            /* no tail */
            if (ltok <= 0) {
                logmsg(LOGMSG_ERROR, "ptranfail command requires 'on' or 'off' argument\n");
                logmsg(LOGMSG_USER, "ptranfail test is %s\n",
                       (TCM_PARENT_DEADLOCK == tcmtest_flag) ? "*enabled*"
                                                             : "*disabled*");
            }

            /* enable this test */
            else if (tokcmp(tok, ltok, "on") == 0) {
                tcmtest_flag = TCM_PARENT_DEADLOCK;
                logmsg(LOGMSG_USER, "enabled tcm parent-tran deadlock test\n");
            }

            /* disable this test */
            else if (tokcmp(tok, ltok, "off") == 0) {
                if (TCM_PARENT_DEADLOCK == tcmtest_flag) {
                    tcmtest_flag = 0;
                    logmsg(LOGMSG_USER, "disabled tcm parent-tran deadlock test\n");
                } else {
                    logmsg(LOGMSG_USER, "tcm parent-tran deadlock test is not enabled\n");
                }
            }

            /* garbage tail */
            else {
                logmsg(LOGMSG_ERROR, "ptranfail command requires 'on' or 'off' argument\n");
                logmsg(LOGMSG_USER, "ptranfail test is %s\n",
                       (TCM_PARENT_DEADLOCK == tcmtest_flag) ? "*enabled*"
                                                             : "*disabled*");
            }
        }

    } else if (tokcmp(tok, ltok, "nodeix") == 0) {
        const char *hosts[REPMAX];
        int numnodes;

        numnodes = net_get_all_nodes_connected(thedb->handle_sibling, hosts);

        hosts[numnodes] = gbl_mynode;
        numnodes++;

        logmsg(LOGMSG_USER, "nodes:\n");
        for (int i = 0; i < numnodes; i++)
            logmsg(LOGMSG_USER, "  %s %d\n", hosts[i], nodeix(hosts[i]));
    } else if (tokcmp(tok, ltok, "help") == 0) {
        logmsg(LOGMSG_USER, "tcmtest <test>       - enable a cdb2tcm test\n");
        logmsg(LOGMSG_USER, "tcmtest list         - list cdb2tcm tests\n");
        logmsg(LOGMSG_USER, "getvers table        - get schema version for table (or all)\n");
        logmsg(LOGMSG_USER, "putvers table num    - set schema version for table\n");
        logmsg(LOGMSG_USER, "delsc   table tag    - delete a tag\n");
    } else {
        logmsg(LOGMSG_ERROR, "Unknown debug command <%.*s>\n", ltok, tok);
    }
}

