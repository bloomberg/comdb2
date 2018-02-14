/*
   Copyright 2017 Bloomberg Finance L.P.

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

#ifndef __INCLUDED_COMDB2_PLUGIN_H
#define __INCLUDED_COMDB2_PLUGIN_H

enum comdb2_plugin_type {
    COMDB2_PLUGIN_APPSOCK,
    COMDB2_PLUGIN_OPCODE,
    COMDB2_PLUGIN_MACHINE_INFO,
    COMDB2_PLUGIN_INITIALIZER,
    COMDB2_PLUGIN_LAST
};

enum comdb2_plugin_flag {
    /* Flag to indicate whether the plugin is static. */
    COMDB2_PLUGIN_STATIC = 1,
};

struct comdb2_plugin {
    const char *name;        /* Plugin name */
    const char *descr;       /* Plugin description */
    int type;                /* Plugin type */
    int version;             /* Plugin version */
    int iface_version;       /* Plugin interface version */
    int flags;               /* Plugin flags */
    int (*init_cb)(void *);  /* Initialization function */
    int (*destroy_cb)(void); /* Destroy function */
    void *data;              /* Plugin-specific data */
};
typedef struct comdb2_plugin comdb2_plugin_t;

const char *comdb2_plugin_type_to_str(int type);

#endif /* ! __INCLUDED_COMDB2_PLUGIN_H */
