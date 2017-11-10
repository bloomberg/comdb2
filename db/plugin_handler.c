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

/* Maximum lenth of shared-object file name. */
#define COMDB2_MAX_PATH_NAME_LEN 100

#include <assert.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>
#include "cdb2_constants.h"
#include "comdb2.h"
#include "logmsg.h"
#include "tunables.h"
#include "comdb2_plugin.h"
#include "comdb2_appsock.h"
#include "comdb2_opcode.h"
#include "comdb2_machine_info.h"
#include "rtcpu.h"
#include "all_plugins.h"

/* All registered plugins */
comdb2_plugin_t **gbl_plugins;

/* Total number of plugins installed. */
int gbl_plugin_count;

/* If specified, the plugins will be loaded from this directory. */
static char *plugindir;

extern hash_t *gbl_appsock_hash;
extern hash_t *gbl_opcode_hash;

static int install_plugin_int(comdb2_plugin_t *new_plugin)
{
    comdb2_plugin_t *plugin = NULL;
    int i;

    assert(new_plugin);

    for (i = 0; gbl_plugins[i]; ++i) {
        if (strcmp(gbl_plugins[i]->name, new_plugin->name) == 0) {
            /* Plugin already loaded, override it with a warning. */
            logmsg(LOGMSG_WARN, "Plugin already installed (%s) overriding..",
                   plugin->name);

            plugin = gbl_plugins[i];

            /* Destroy the already installed plugin and reuse the slot */
            if (plugin->destroy_cb) {
                plugin->destroy_cb();
            }
            break;
        }
    }

    if (i >= MAXPLUGINS) {
        logmsg(LOGMSG_ERROR, "Maximum number of plugins already installed. "
                             "Ignoring further requests to install plugin(s).");
        return 1;
    }

    /* TODO: Check plugin version */

    if (new_plugin->type >= COMDB2_PLUGIN_LAST) {
        logmsg(LOGMSG_ERROR, "Invalid plugin type for %s", new_plugin->name);
        return 1;
    }

    /*
      Initialize the plugin and add it to the global list of installed plugins.
    */
    if (new_plugin->init_cb && new_plugin->init_cb(NULL)) {
        logmsg(LOGMSG_ERROR, "Plugin initialization failed (%s).",
               new_plugin->name);
        return 1;
    }

    switch (new_plugin->type) {
    case COMDB2_PLUGIN_APPSOCK: {
        comdb2_appsock_t *appsock;
        appsock = (comdb2_appsock_t *)new_plugin->data;

        /*
          Check whether a similar appsock handler has already been
          added to the hash.
        */
        if (hash_find_readonly(gbl_appsock_hash, &appsock->name)) {
            logmsg(LOGMSG_FATAL, "duplicate appsock handler found\n");
            return 1;
        }
        hash_add(gbl_appsock_hash, appsock);
        break;
    }
    case COMDB2_PLUGIN_OPCODE: {
        comdb2_opcode_t *opcode;
        opcode = (comdb2_opcode_t *)new_plugin->data;

        /*
          Check whether a similar opcode handler has already been
          added to the hash.
        */
        if (hash_find_readonly(gbl_opcode_hash, &opcode->name)) {
            logmsg(LOGMSG_FATAL, "duplicate opcode handler found\n");
            return 1;
        }
        hash_add(gbl_opcode_hash, opcode);
        break;
    }
    case COMDB2_PLUGIN_MACHINE_INFO: {
        comdb2_machine_info_t *machine_info;
        machine_info = (comdb2_machine_info_t *)new_plugin->data;
        register_rtcpu_callbacks(
            machine_info->machine_is_up, machine_info->machine_status_init,
            machine_info->machine_class, machine_info->machine_dc);
        break;
    }
    default:
        logmsg(LOGMSG_ERROR, "Invalid plugin %s.", plugin->name);
        return 1;
    }

    gbl_plugins[i] = new_plugin;

    /* Increment the count only if an warlier plugin was not replaced. */
    if (!plugin)
        ++gbl_plugin_count;

    logmsg(LOGMSG_INFO, "Plugin '%s' installed.", new_plugin->name);

    return 0;
}

static int install_plugin(const char *file_name, const char *plugin_name)
{
    void *handle;
    comdb2_plugin_t *plugin;
    comdb2_plugin_t *tmp;
    int i;

    handle = dlopen(file_name, RTLD_LAZY);
    if (!handle) {
        logmsg(LOGMSG_FATAL, "dlopen() failed: %s", dlerror());
        exit(1);
    }

    plugin = (comdb2_plugin_t *)dlsym(handle, "comdb2_plugin");
    if (!plugin) {
        logmsg(LOGMSG_FATAL, "dlsym() failed: %s", dlerror());
        exit(1);
    }

    tmp = NULL;
    while (plugin->name) {
        if (strcmp(plugin_name, plugin->name) == 0) {
            tmp = plugin;
            break;
        }
        ++plugin;
    }

    if (tmp == NULL) {
        logmsg(LOGMSG_ERROR, "Plugin %s not found in the shared object file.",
               plugin_name);
        return 1;
    } else if (install_plugin_int(tmp)) {
        logmsg(LOGMSG_ERROR, "Failed to install plugin %s.", plugin_name);
        return 1;
    }

    return 0;
}

static int install_all_plugins(const char *file_name)
{
    void *handle;
    comdb2_plugin_t *plugin;
    int i;

    handle = dlopen(file_name, RTLD_LAZY);
    if (!handle) {
        logmsg(LOGMSG_FATAL, "dlopen() failed: %s", dlerror());
        exit(1);
    }

    plugin = (comdb2_plugin_t *)dlsym(handle, "comdb2_plugin");
    if (!plugin) {
        logmsg(LOGMSG_FATAL, "dlsym() failed: %s", dlerror());
        exit(1);
    }

    while (plugin->name) {
        if (install_plugin_int(plugin)) {
            logmsg(LOGMSG_ERROR, "Failed to install plugin %s.", plugin->name);
            return 1;
        }
        ++plugin;
    }

    return 0;
}

int install_builtin_plugins(void)
{
    comdb2_plugin_t *plugin;
    for (int i = 0; i < sizeof(all_plugins) / sizeof(comdb2_plugin_t *); ++i) {
        plugin = all_plugins[i];
        while (plugin->name) {
            plugin->flags |= COMDB2_PLUGIN_STATIC;
            if (install_plugin_int(plugin)) {
                logmsg(LOGMSG_ERROR, "Failed to install plugin %s.",
                       plugin->name);
                return 1;
            }
            ++plugin;
        }
    }
    return 0;
}

static int plugin_update(void *context, void *value)
{
    char *ptr, *saveptr;
    char path[COMDB2_MAX_PATH_NAME_LEN];
    int rc = 0;
    /* Assume only path has been provided. */
    int path_only = 1;

    ptr = strtok_r((char *)value, ":", &saveptr);

    if (ptr) {
        strncpy(path, ptr, COMDB2_MAX_PATH_NAME_LEN);
    }

    while (!rc && (ptr = strtok_r(NULL, ",", &saveptr))) {
        path_only = 0;
        rc = install_plugin(path, ptr);
    }

    if (path_only == 1) {
        /*
          No plugin name specified, load all the plugins from the given
          shared object file.
        */
        rc = install_all_plugins(path);
    }

    return rc;
}

/* Register all plugin tunables. */
void register_plugin_tunables(void)
{
    REGISTER_TUNABLE("plugin",
                     "Load plugin from the specified shared object file.",
                     TUNABLE_STRING, NULL, READONLY | INTERNAL, NULL, NULL,
                     plugin_update, NULL);
    REGISTER_TUNABLE(
        "plugindir", "Default directory to look for shared object files.",
        TUNABLE_STRING, &plugindir, READONLY, NULL, NULL, NULL, NULL);
}

/* Initialize the plugin sub-system. */
int init_plugins(void)
{
    gbl_plugins =
        (comdb2_plugin_t **)calloc(MAXPLUGINS + 1, sizeof(comdb2_plugin_t *));
    if (!gbl_plugins) {
        logmsg(LOGMSG_ERROR, "System out of memory.");
        return 1;
    }

    return 0;
}

/* Destroy all plugins. */
int destroy_plugins(void)
{
    return 0;
}

const char *comdb2_plugin_type_to_str(int type)
{
    switch (type) {
    case COMDB2_PLUGIN_APPSOCK:
        return "appsock";
    case COMDB2_PLUGIN_OPCODE:
        return "opcode";
    case COMDB2_PLUGIN_MACHINE_INFO:
        return "machine_info";
    default:
        break;
    }
    return "unknown";
}
