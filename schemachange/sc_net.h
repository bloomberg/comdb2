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

#ifndef INCLUDE_SC_NET_H
#define INCLUDE_SC_NET_H

int appsock_schema_change(SBUF2 *sb, int *keepsocket);

void handle_setcompr(SBUF2 *sb);

void vsb_printf(SBUF2 *sb, const char *sb_prefix, const char *prefix,
                const char *fmt, va_list args);
void sb_printf(SBUF2 *sb, const char *fmt, ...);
void sb_errf(SBUF2 *sb, const char *fmt, ...);

void sc_printf(struct schema_change_type *s, const char *fmt, ...);
void sc_errf(struct schema_change_type *s, const char *fmt, ...);

#endif
