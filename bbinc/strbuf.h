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

#ifndef STRBUF_H
#define STRBUF_H
#include <stdarg.h>

typedef struct strbuf strbuf;
strbuf *strbuf_new(void);
void strbuf_append(strbuf *, const char *);
void strbuf_append_with_escape(strbuf *, const char *, char);
void strbuf_appendf(strbuf *, const char *, ...);
void strbuf_vappendf(strbuf *, const char *, va_list args);
void strbuf_clear(strbuf *);
void strbuf_free(strbuf *);
void strbuf_hex(strbuf *, void *buf, int len);
const char *strbuf_buf(const strbuf *);
int strbuf_len(const strbuf *buf);
char *strbuf_disown(strbuf *);
void strbuf_del(strbuf *buf, int n);

#endif
