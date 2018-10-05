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

#include <stdio.h>
#include <stdarg.h>
#include <epochlib.h>
#include "comdb2.h"

/* flush output. */
void reqprintflush(struct ireq *iq)
{
    reqlog_logl(iq->reqlogger, REQL_TRACE, "\n");
}

/* prints to debug string until full.  FLUSHES OUTPUT FIRST..*/
void reqmoref(struct ireq *iq, char *format, ...)
{
    int rc;
    va_list ap;
    va_start(ap, format);
    reqlog_logv(iq->reqlogger, REQL_TRACE, format, ap);
    va_end(ap);
}

/* continues string...*/
void reqprintf(struct ireq *iq, char *format, ...)
{
    int rc;
    va_list ap;
    va_start(ap, format);
    reqprintflush(iq);
    reqlog_logv(iq->reqlogger, REQL_TRACE, format, ap);
    va_end(ap);
}

void reqerrstr(struct ireq *iq, int rc, char *format, ...)
{
    va_list ap;
    va_start(ap, format);

    /* check errstr should be reported */
    if (!iq->errstrused) {
        va_end(ap);
        return;
    }

    /* save errstr */
    errstat_set_rc(&iq->errstat, rc);
    errstat_set_strfap(&iq->errstat, format, ap);

    va_end(ap);
}

void reqerrstrhdr(struct ireq *iq, char *format, ...)
{
    va_list ap;
    va_start(ap, format);

    /* concatenate header */
    errstat_cat_hdrfap(&iq->errstat, format, ap);

    va_end(ap);
}

void reqerrstrclr(struct ireq *iq) { errstat_clr(&iq->errstat); }

void reqerrstrhdrclr(struct ireq *iq) { errstat_clr_hdr(&iq->errstat); }

void reqdumphex(struct ireq *iq, void *buf, int nb)
{
    int ii;
    unsigned char *uu = (unsigned char *)buf;
    static char *digits = "0123456789abcdef";
    for (ii = 0; ii < nb; ii++) {
        reqmoref(iq, "%c%c", digits[((uu[ii] & 0xf0) >> 4)],
                 digits[(uu[ii] & 0x0f)]);
    }
}

void reqpushprefixf(struct ireq *iq, const char *format, ...)
{
    int left;
    va_list ap;
    va_start(ap, format);
    reqlog_pushprefixv(iq->reqlogger, format, ap);
    va_end(ap);
}

void reqpopprefixes(struct ireq *iq, int num)
{
    if (num == -1)
        reqlog_popallprefixes(iq->reqlogger);
    else
        while (num > 0) {
            reqlog_popprefix(iq->reqlogger);
            num--;
        }
}
