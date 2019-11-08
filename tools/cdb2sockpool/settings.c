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

#include <strings.h>
#include <cdb2sockpool.h>

struct setting {
    unsigned *variable;
    enum {
        TYPE_NONE,
        TYPE_BOOL,
        TYPE_VALUE,
        TYPE_MSECS,
        TYPE_SECS,
        TYPE_BYTES
    } type;
    const char *name;
    const char *descr;
    unsigned default_value;
};

/* Declare all setting variables */
#define SETTING_MACRO(var, dfl, descr, type) unsigned var = dfl;
#include "settings.h"
#undef SETTING_MACRO

#define SETTING_MACRO(var, dfl, descr, type) {&var, TYPE_##type, #var, descr, dfl},
static struct setting settings[] = {
#include "settings.h"
    {NULL, TYPE_NONE, NULL, NULL, 0}
};
#undef SETTING_MACRO

#include "strbuf.h"
#include <syslog.h>

void
print_setting(const struct setting *setting)
{
    strbuf *stb = strbuf_new();
    strbuf_appendf(stb, "%-32s = %u", setting->name, *(setting->variable));
    switch(setting->type) {
        case TYPE_BOOL:
            strbuf_appendf(stb, " (boolean)");
            break;
        case TYPE_BYTES:
            strbuf_appendf(stb, " bytes");
            break;
        case TYPE_SECS:
            strbuf_appendf(stb, " secs");
            break;
        case TYPE_MSECS:
            strbuf_appendf(stb, " ms");
            break;
        default:
            /* no special suffix */
            break;
    }
    while (strbuf_len(stb) < 50) {
        strbuf_append(stb, " ");
    }
    strbuf_appendf(stb, " %s\n", setting->descr);
    syslog(LOG_INFO, "%s", strbuf_buf(stb));
    strbuf_free(stb);
}

void
print_all_settings(void)
{
    int ii;
    for(ii = 0; settings[ii].variable; ii++) {
        print_setting(&settings[ii]);
    }
}

int
set_setting(const char *name, unsigned new_value)
{
    int ii;
    for(ii = 0; settings[ii].variable; ii++) {
        if(strcasecmp(settings[ii].name, name) == 0) {
            if(settings[ii].type == TYPE_BOOL && new_value > 1) {
                syslog(LOG_WARNING, "expected 0 or 1 for a bool setting\n");
                return -1;
            }

            *(settings[ii].variable) = new_value;
            setting_changed(settings[ii].variable);
            print_setting(&settings[ii]);

            return 0;
        }
    }
    syslog(LOG_WARNING, "unknown setting %s\n", name);
    return -1;
}
