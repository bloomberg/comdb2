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

#include <fsnapf.h>

#include <ctype.h>

static void fsnapi(const char *prefix, const char *buf, size_t len,
                   fsnap_callback_type callback, void *context)
{
    static char *hexchars = "0123456789ABCDEF";
    char hdsp[60], cdsp[60], ch;
    size_t ii, jj, boff, hoff, coff;
    if (!prefix)
        prefix = "";
    for (ii = 0; ii < len; ii += 16) {
        hoff = coff = 0;
        for (jj = 0; jj < 16; jj++) {
            boff = ii + jj;
            if (boff >= len) {
                hdsp[hoff++] = ' ';
                hdsp[hoff++] = ' ';
                cdsp[coff++] = ' ';
            } else {
                ch = buf[boff];
                hdsp[hoff++] = hexchars[(ch >> 4) & 0x0f];
                hdsp[hoff++] = hexchars[ch & 0x0f];
                if (ch >= ' ' && ch < 0x7f)
                    cdsp[coff++] = ch;
                else
                    cdsp[coff++] = '.';
            }
            if ((jj & 3) == 3)
                hdsp[hoff++] = ' ';
        }
        hdsp[hoff] = 0;
        cdsp[coff] = 0;
        if (callback(context, "%s%5x:%s |%s|\n", prefix, ii, hdsp, cdsp) < 0)
            return;
    }
}

void fsnapf(FILE *fil, const void *buf, int len)
{
    if (len < 0)
        return;
    fsnapi(NULL, buf, len, (fsnap_callback_type)fprintf, fil);
}

void fsnapp(const char *prefix, const void *buf, size_t len,
            fsnap_callback_type callback, void *context)
{
    fsnapi(prefix, buf, len, callback, context);
}

void fsnap_prfx(FILE *fil, const char *prfx, const void *buf, size_t len)
{
    fsnapi(prfx, buf, len, (fsnap_callback_type)fprintf, fil);
}
