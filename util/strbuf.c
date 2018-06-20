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

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <assert.h>

#include "strbuf.h"
#ifndef BUILDING_TOOLS
#include "mem_util.h"
#include "mem_override.h"
#endif
#include "logmsg.h"

#define STRBUF_INC 64

struct strbuf {
    char *buf;
    size_t len;
    size_t alloc;
};

strbuf *strbuf_new(void)
{
    strbuf *buf = malloc(sizeof(strbuf));
    if (buf == 0) {
        logmsg(LOGMSG_FATAL, "%s: memory allocation failed\n", __func__);
        abort();
    }
    buf->buf = malloc(STRBUF_INC);
    if (buf->buf == 0) {
        logmsg(LOGMSG_FATAL, "%s: memory allocation failed\n", __func__);
        abort();
    }
    buf->buf[0] = '\0';
    buf->alloc = STRBUF_INC;
    buf->len = 0;
    return buf;
}

void strbuf_append(strbuf *buf, const char *str)
{
    assert(buf != NULL);
    size_t len = strlen(str);
    if (buf->alloc < buf->len + len + 1) {
        size_t inc = len >= STRBUF_INC ? len + 1 : STRBUF_INC;
        buf->alloc += inc;
        buf->buf = realloc(buf->buf, buf->alloc);
        if (buf->buf == 0) {
            logmsg(LOGMSG_FATAL, "%s: memory allocation failed\n", __func__);
            abort();
        }
    }
    strcat(buf->buf, str);
    buf->len += len;
}

const char *strbuf_buf(const strbuf *buf)
{
    assert(buf != NULL);
    return buf->buf;
}

int strbuf_len(const strbuf *buf)
{
    assert(buf != NULL);
    return buf->len;
}

void strbuf_clear(strbuf *buf)
{
    assert(buf != NULL);
    if (buf->buf) {
        buf->buf[0] = '\0';
    }
    buf->len = 0;
}

void strbuf_free(strbuf *buf)
{
    assert(buf != NULL);
    if (buf->buf)
        free(buf->buf);
    free(buf);
}

char *strbuf_disown(strbuf *buf)
{
    assert(buf != NULL);
    char *c = buf->buf;
    buf->buf = NULL;
    buf->len = buf->alloc = 0;
    return c;
}

void strbuf_del(strbuf *buf, int n)
{
    if (n > buf->len)
        return;
    while (n > 0) {
        buf->buf[--buf->len] = 0;
        n--;
    }
}

void strbuf_vappendf(strbuf *buf, const char *fmt, va_list args)
{
    char s[1];
    int len;
    char *out;

    len = vsnprintf(s, 1, fmt, args);
    if (len <= 0) {
        return;
    }
    len++;
    out = malloc(len);
    if (out == NULL) {
        logmsg(LOGMSG_FATAL, "%s: memory allocation failed\n", __func__);
        abort();
    }
    vsnprintf(out, len, fmt, args);
    strbuf_append(buf, out);
    free(out);
}

void strbuf_appendf(strbuf *buf, const char *fmt, ...)
{
    char s[1];
    va_list args;
    int len;
    char *out;

    va_start(args, fmt);
    len = vsnprintf(s, 1, fmt, args);
    va_end(args);
    if (len <= 0) {
        return;
    }
    len++;
    out = malloc(len);
    if (out == NULL) {
        logmsg(LOGMSG_FATAL, "%s: memory allocation failed\n", __func__);
        abort();
    }
    va_start(args, fmt);
    vsnprintf(out, len, fmt, args);
    va_end(args);
    strbuf_append(buf, out);
    free(out);
}
