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
#include <ctype.h>
#include <stddef.h>

#ifndef BUILDING_TOOLS
#include <mem_util.h>
#include <mem_override.h>
#endif

static inline char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}

void hexdumpbuf(const char *key, int keylen, char **buf)
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

int util_tobytes(char *out, const char *hex, size_t len)
{
    if (!hex || !out)
        return -1;

    /* skip optional 0x / 0X */
    if (hex[0] == '0' && (hex[1] == 'x' || hex[1] == 'X'))
        hex += 2;

    int hi = -1;
    size_t n = 0;

    for (; *hex; hex++) {
        if (isspace((unsigned char)*hex))
            continue;

        int v;
        if (*hex >= '0' && *hex <= '9')
            v = *hex - '0';
        else if (*hex >= 'a' && *hex <= 'f')
            v = 10 + (*hex - 'a');
        else if (*hex >= 'A' && *hex <= 'F')
            v = 10 + (*hex - 'A');
        else
            return -1; /* invalid character */

        if (hi < 0) {
            hi = v;
        } else {
            if (n >= len)
                return -1;
            out[n++] = (unsigned char)((hi << 4) | v);
            hi = -1;
        }
    }

    /* odd number of hex digits is invalid */
    return (hi == -1) ? 0 : -1;
}

void hexdump(loglvl lvl, const char *key, int keylen)
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

void hexdumpfp(FILE *fp, const unsigned char *key, int keylen)
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

void print_hex_nl(const uint8_t *b, unsigned l, int newline)
{
    hexdumpfp(stdout, b, l);
    if (newline) fprintf(stdout, "\n");
}

void print_hex(const uint8_t *b, unsigned l)
{
    print_hex_nl(b, l, 1);
}

#include <dbinc/fileid_len.h>

void fileid_str(u_int8_t *fileid, char *str)
{
    char *p = str;
    u_int8_t *f = fileid;
    for (int i = 0; i < DB_FILE_ID_LEN; i++, f++, p += 2) {
        sprintf(p, "%2.2x", (u_int)*f);
    }
}
