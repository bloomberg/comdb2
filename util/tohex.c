/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include <tohex.h>
#include <stdlib.h>
#include <alloca.h>

static inline char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}

void hexdumpbuf(char *key, int keylen, char **buf)
{
    char *mem;
    char *output;

    mem = malloc((2 * keylen) + 2);
    output = util_tohex(mem, key, keylen);

    *buf = output;
}

/* Return a hex string
 * output buffer should be appropriately sized */
char *util_tohex(char *out, const char *in, size_t len)
{
    char *beginning = out;
    char hex[] = "0123456789abcdef";
    const char *end = in + len;
    while (in != end) {
        char i = *(in++);
        *(out++) = hex[(i & 0xf0) >> 4];
        *(out++) = hex[i & 0x0f];
    }
    *out = 0;

    return beginning;
}

void hexdump(loglvl lvl, unsigned char *key, int keylen)
{
    char *mem;
    char *output;

    if (key == NULL || keylen == 0) {
        logmsg(LOGMSG_ERROR, "NULL(%d)\n", keylen);
        return;
    }
    if (keylen > 1000)
        mem = (char *)malloc((2 * keylen) + 2);
    else
        mem = (char *)alloca((2 * keylen) + 2);
    output = util_tohex(mem, (const char *)key, keylen);
    logmsg(lvl, "%s\n", output);

    if (keylen > 1000)
        free(mem);
}

/* printf directly (for printlog) */
void hexdumpdbt(DBT *dbt)
{
    unsigned char *s = dbt->data;
    int len = dbt->size;

    while (len) {
        printf("%02x", *s);
        s++;
        len--;
    }
}

void hexdumpfp(FILE *fp, unsigned char *key, int keylen)
{
    int i = 0;
    for (i = 0; i < keylen; i++) {
        if (fp) {
            fprintf(fp, "%c%c", hex(((unsigned char)key[i]) / 16),
                    hex(((unsigned char)key[i]) % 16));
        } else {
            logmsg(LOGMSG_USER, "%c%c", hex(((unsigned char)key[i]) / 16),
                   hex(((unsigned char)key[i]) % 16));
        }
    }
}
