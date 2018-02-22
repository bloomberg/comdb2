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

/* Some signal handling utilities. */

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cdb2_constants.h"
#include "ctrace.h"
#include "sigutil.h"
#include "logmsg.h"

struct signal_name {
    int signum;
    const char *name;
};

#define defsig(name)                                                           \
    {                                                                          \
        SIG##name, #name                                                       \
    }
const struct signal_name unix_signals[] = {
    defsig(HUP),    defsig(INT),     defsig(QUIT),     defsig(ILL),
    defsig(TRAP),   defsig(ABRT),    defsig(FPE),      defsig(KILL),
    defsig(BUS),    defsig(SEGV),    defsig(SYS),      defsig(PIPE),
    defsig(ALRM),   defsig(TERM),    defsig(URG),      defsig(STOP),
    defsig(TSTP),   defsig(CONT),    defsig(CHLD),     defsig(TTIN),
    defsig(TTOU),   defsig(IO),      defsig(XCPU),     defsig(XFSZ),
    defsig(VTALRM), defsig(USR1),    defsig(USR2),     defsig(WINCH),
#if !defined(__APPLE__)
    defsig(PWR),
#endif
#if defined(_IBM_SOURCE) || defined(_SUN_SOURCE)
    defsig(EMT),    defsig(WAITING),
#endif /* _IBM_SOURCE || _SUN_SOURCE */
#if defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)
    defsig(PROF),
#endif /* _IBM_SOURCE || _LINUX_SOURCE */
#ifdef _IBM_SOURCE
    defsig(MSG),    defsig(DANGER),  defsig(MIGRATE),  defsig(PRE),
    defsig(VIRT),   defsig(ALRM1),   defsig(RECONFIG), defsig(CPUFAIL),
    defsig(GRANT),  defsig(RETRACT), defsig(SOUND),    defsig(SAK),
#endif /*_IBM_SOURCE*/
#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)
#   if !defined(__APPLE__)
    defsig(CLD),
#   endif
#endif /* _SUN_SOURCE || _LINUX_SOURCE */
#ifdef _SUN_SOURCE
    defsig(LWP),    defsig(FREEZE),  defsig(THAW),     defsig(CANCEL),
    defsig(LOST),   defsig(XRES),
#endif /*_SUN_SOURCE*/
#ifdef _LINUX_SOURCE
    defsig(ABRT),   defsig(ALRM),    defsig(IOT),
#   if !defined(__APPLE__)
    defsig(POLL),   defsig(STKFLT),
#   endif
#endif /*_LINUX_SOURCE*/
};

const int num_unix_signals = sizeof(unix_signals) / sizeof(unix_signals[0]);

const char *signum2a(int signum)
{
    int ii;
    for (ii = 0; ii < num_unix_signals; ii++)
        if (unix_signals[ii].signum == signum)
            return unix_signals[ii].name;
    return "unknown";
}

void sprintsigact(char *buf, size_t buflen, const struct sigaction *act)
{
    const static int flags[] = {SA_ONSTACK,  SA_RESETHAND, SA_NODEFER,
                                SA_RESTART,  SA_SIGINFO,   SA_NOCLDWAIT,
                                SA_NOCLDSTOP};
    const static char *flagstrs[] = {
        "SA_ONSTACK", "SA_RESETHAND", "SA_NODEFER",  "SA_RESTART",
        "SA_SIGINFO", "SA_NOCLDWAIT", "SA_NOCLDSTOP"};
    int ii;
    char fstr[128] = "";
    int pos;
    char hstr[64] = "NULL";
    char mstr[1024] = "";
    if (act->sa_handler != NULL)
        snprintf(hstr, sizeof(hstr) - 1, "@%p", act->sa_handler);

    pos = 0;
    for (ii = 0; ii < num_unix_signals; ii++) {
        int signo = unix_signals[ii].signum;
        if (sigismember(&act->sa_mask, signo)) {
            if (pos > 0)
                mstr[pos++] = '|';
            SNPRINTF(mstr, sizeof(mstr), pos, "%s", unix_signals[ii].name);
        }
    }

    pos = 0;
    for (ii = 0; ii < sizeof(flags) / sizeof(flags[0]); ii++) {
        if (act->sa_flags & flags[ii]) {
            if (pos > 0)
                fstr[pos++] = '|';
            SNPRINTF(fstr, sizeof(fstr), pos, "%s", flagstrs[ii]);
        }
    }

done:
    snprintf(buf, buflen, "<sa_handler=%s, sa_mask=[%s], sa_flags=%s>", hstr,
             mstr, fstr);
}

void dumpsignalsetupf(FILE *out)
{
    int ii;
    struct sigaction act;

    for (ii = 0; ii < num_unix_signals; ii++) {
        int signo = unix_signals[ii].signum;
        if (sigaction(signo, NULL, &act) == -1)
            logmsg(LOGMSG_ERROR, "dumpsignalsetup: sigaction(%d/%s) failed: %d %s\n",
                    signo, unix_signals[ii].name, errno, strerror(errno));
        else {
            char buf[1024];
            sprintsigact(buf, sizeof(buf), &act);
            logmsg(LOGMSG_USER, "%8s = %s\n", unix_signals[ii].name, buf);
        }
    }
}

void ctracesignalsetup(void)
{
    int ii;
    struct sigaction act;

    for (ii = 0; ii < num_unix_signals; ii++) {
        int signo = unix_signals[ii].signum;
        if (sigaction(signo, NULL, &act) == -1)
            logmsg(LOGMSG_ERROR, "dumpsignalsetup: sigaction(%d/%s) failed: %d %s\n",
                    signo, unix_signals[ii].name, errno, strerror(errno));
        else {
            char buf[1024];
            sprintsigact(buf, sizeof(buf), &act);
            ctrace("%8s = %s\n", unix_signals[ii].name, buf);
        }
    }
}
