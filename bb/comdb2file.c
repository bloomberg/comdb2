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
#include <ctype.h>

#include "util.h"
#include "plhash.h"

#include "mem_bb.h"
#include "mem_override.h"

char *gbl_config_root = NULL;
static hash_t *locations;

struct location {
    char *type;
    char *dir;
};

#define LOCATION_SEP " \t\n"

static void add_location(char *type, char *dir);
static int load_locations_from(char *path);

static void add_location(char *type, char *dir)
{
    struct location *l;

    l = hash_find(locations, &type);
    if (l) {
        free(l->type);
        free(l->dir);
        hash_del(locations, l);
        free(l);
    }
    l = malloc(sizeof(struct location));
    l->type = strdup(type);
    l->dir = strdup(dir);
    hash_add(locations, l);
}

static int load_locations_from(char *path)
{
    char line[1024];
    FILE *f;
    char *e, *tok;
    char *s;
    int linenum = 0;
    char *type, *dir;

    f = fopen(path, "r");
    if (f == NULL)
        return 1;

    while (fgets(line, sizeof(line), f)) {
        linenum++;
        s = line;
        while (isspace(*s))
            s++;
        if (*s == '#' || *s == '\n')
            continue;
        tok = strtok_r(s, LOCATION_SEP, &e);
        if (tok == NULL)
            continue;
        if (strcmp(tok, "location") == 0) {
            tok = strtok_r(NULL, LOCATION_SEP, &e);
            if (tok == NULL)
                continue;
            type = tok;
            tok = strtok_r(NULL, LOCATION_SEP, &e);
            if (tok == NULL)
                continue;
            dir = tok;
            add_location(type, dir);
        }
    }
    fclose(f);
    return 0;
}

/* returns a malloced string which caller needs to free */
char *comdb2_location(char *type, char *fmt, ...)
{
    struct location *l;
    va_list args;
    char *out;

    l = hash_find_readonly(locations, &type);
    if (l == NULL) {
        va_list args;
        if (fmt) {
            va_start(args, fmt);
            out = comdb2_filev(fmt, args);
            va_end(args);
        } else {
            out = comdb2_file(type);
        }
    } else {
        if (fmt) {
            char *fmtout;
            va_start(args, fmt);
            fmtout = comdb2_vasprintf(fmt, args);
            va_end(args);
            out = comdb2_asprintf("%s/%s", l->dir, fmtout);
            free(fmtout);
        } else {
            out = strdup(l->dir);
        }
    }
    return out;
}

#define DEFAULT_LOCATION(type, file)                                           \
    do {                                                                       \
        char *f;                                                               \
        f = comdb2_asprintf("%s/%s", gbl_config_root, file);                   \
        add_location(type, f);                                                 \
        free(f);                                                               \
    } while (0)

#define QUOTE_(x) #x
#define QUOTE(x) QUOTE_(x)

void init_file_locations(char *lrlname)
{
    char *fname;
    char *dbhome;
    char *global_config;

    locations = hash_init_strptr(offsetof(struct location, type));

    gbl_config_root = getenv("COMDB2_ROOT");
    if (gbl_config_root == NULL)
        gbl_config_root = QUOTE(COMDB2_ROOT); /* configured at build */
    if (strlen(gbl_config_root) == 0)         /* not set */
        gbl_config_root = "/";

    /* init defaults */
    DEFAULT_LOCATION("logs", "var/log/cdb2");
    DEFAULT_LOCATION("marker", "tmp/cdb2");
    DEFAULT_LOCATION("debug", "var/log/cdb2");
    DEFAULT_LOCATION("tmp", "tmp/cdb2");
    DEFAULT_LOCATION("config", "etc/cdb2/config");
    DEFAULT_LOCATION("scripts", "bin");
    DEFAULT_LOCATION("rtcpu", "etc/cdb2/rtcpu");
    DEFAULT_LOCATION("share", "share/cdb2");
    dbhome = getenv("COMDB2_DBHOME");
    if (dbhome)
        add_location("database", dbhome);
    else
        DEFAULT_LOCATION("database", "var/cdb2");


#if defined(_LINUX_SOURCE)
    add_location("tzdata", "/usr/share/");
#elif defined(_IBM_SOURCE)
    add_location("tzdata", "/usr/share/lib");
#elif defined(_SUN_SOURCE)
    add_location("tzdata", "/usr/share/lib");
#endif

    global_config = getenv("COMDB2_GLOBAL_CONFIG");
    if (global_config)
        fname = comdb2_location("config", "%s/comdb2.lrl", global_config);
    else
        fname = comdb2_location("config", "etc/cdb2/config/comdb2.lrl");

    if (load_locations_from(fname)) {
        /* don't start if missing? */
    }
    free(fname);
    fname = comdb2_location("comdb2_local.lrl", NULL);
    load_locations_from(fname);
    free(fname);
    if (lrlname)
        load_locations_from(lrlname);
}

char *comdb2_filev(char *fmt, va_list args)
{
    int len;
    char *fname;
    char b[1];
    va_list argscpy;

    va_copy(argscpy, args);

    len = vsnprintf(b, 1, fmt, args);
    len += strlen(gbl_config_root) + 1 + 1;

    fname = malloc(len);
    strcpy(fname, gbl_config_root);
    strcat(fname, "/");
    vsnprintf(fname + strlen(fname), len - strlen(fname), fmt, argscpy);
    va_end(argscpy);
    return fname;
}

char *comdb2_file(char *fmt, ...)
{
    char *out;
    va_list args;

    va_start(args, fmt);
    out = comdb2_filev(fmt, args);
    va_end(args);

    return out;
}

void cleanup_file_locations()
{
    if (locations) {
        hash_clear(locations);
        hash_free(locations);
        locations = NULL;
    }
}
