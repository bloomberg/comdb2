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

/*
 * This is a simple resource manager.  The lrl file can reference resources
 * (files) which copycomdb2 will know to copy over.
 *
 * Then other subsystems can access those resources by name.
 *
 * This is really just a gizmo to make Java integration a tiny bit easier.
 *
 * Adding resources is not thread safe and should only be done by the lrl
 * parser.
 */

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>

#include <bb_oscompat.h>
#include <list.h>

#include "comdb2.h"
#include "logmsg.h"

/* The strings are allocated with this structure and so can be freed with it. */
struct resource {
    char *name;
    char *filepath;

    LINKC_T(struct resource) link;
};

static char *lrlname = NULL;
static LISTC_T(struct resource) list;

#if 0
char *strdup(char *str1)
{
   char *ptr;
   ptr = malloc(strlen(str1) +1 );
   strcpy(ptr, str1);
   return ptr;
}
#endif

void initresourceman(const char *newlrlname)
{
    static int once = 1;
    if (once) {
        listc_init(&list, offsetof(struct resource, link));
        once = 0;
    }
    if (!newlrlname)
        return;

    if (lrlname) // free before assigning new one
        free(lrlname);

    lrlname = comdb2_realpath(newlrlname, NULL);

    /* lrl file is always known as "lrl" */
    if (lrlname)
        addresource("lrl", lrlname);
}

/* Gets the path of the child file (usually a .lrl or .csc2 relative to a
 * parent file (the master .lrl).  Returns a pointer to malloc'd memory which
 * should be freed by the caller.
 *
 * e.g.
 *
 * ("/t1/bin/mylrl.lrl", "sometable.csc2")    => "/t1/bin/sometable.csc2"
 * ("/t1/bin/mylrl.lrl", "../sometable.csc2") => "/t1/bin/../sometable.csc2"
 * ("mylrl.lrl",         "../sometable.csc2") => "../sometable.csc2"
 */
char *getdbrelpath(const char *relpath)
{
    char *index, *newpath;
    size_t reltolen, relpathlen;
    const char *relto = lrlname;

    /* if relpath is absolute then return it unaltered -OR- if there is
     * no base path available to help us modify it */
    if (relpath[0] == '/' || !relto)
        return strdup(relpath);

    /* if relto has no path information then return relpath unaltered */
    index = strrchr(relto, '/');
    if (!index)
        return strdup(relpath);

    reltolen = index - relto + 1;
    relpathlen = strlen(relpath);
    newpath = malloc(reltolen + relpathlen + 1);
    memcpy(newpath, relto, reltolen);
    memcpy(newpath + reltolen, relpath, relpathlen);
    newpath[reltolen + relpathlen] = '\0';
    return newpath;
}

void addresource(const char *name, const char *filepath)
{
    if (!name)
        name = filepath;

    struct resource *res;
    /* look for this name and remove it if it is already present */
    LISTC_FOR_EACH(&list, res, link)
    {
        if (strcmp(res->name, name) == 0) {
            logmsg(LOGMSG_INFO, "removing resource %s -> %s\n", res->name, res->filepath);
            listc_rfl(&list, res);
            free(res);
            break;
        }
    }

    int namelen = strlen(name) + 1;
    char *relpath = getdbrelpath(filepath);
    int pathlen = strlen(relpath) + 1;
    res = malloc(sizeof(struct resource) + namelen + pathlen);

    bzero(res, sizeof(struct resource));
    res->name = (char *)(res + 1);
    res->filepath = res->name + namelen;

    if (name)
        memcpy(res->name, name, namelen);
    memcpy(res->filepath, relpath, pathlen);
    free(relpath);

    listc_atl(&list, res);

    logmsg(LOGMSG_INFO, "registered resource %s -> %s\n", res->name, res->filepath);
}

const char *getresourcepath(const char *name)
{
    struct resource *res;
    LISTC_FOR_EACH(&list, res, link)
    {
        if (strcmp(res->name, name) == 0)
            return res->filepath;
    }
    return NULL;
}

// this is called by process_message.c sys.cmd.send("stat resources")
void dumpresources(void)
{
    struct resource *res;
    LISTC_FOR_EACH(&list, res, link)
    {
        logmsg(LOGMSG_USER, "%s -> %s\n", res->name, res->filepath);
    }
}

void cleanresources(void)
{
    void *ent;
    while ((ent = listc_rtl(&list)) != NULL) {
        free(ent);
    }
}

/* Copy every registered resource file (except the auto-added "lrl"
 * self-resource) into `dir`, using each file's basename.  Used by
 * repopulate_lrl() so a repop'd lrl carries copies of its resource files rather than pointing back at
 * the original db directory.  Returns the number of files copied, or -1 on
 * error. */
int dump_qresources(const char *dir)
{
    struct resource *res;
    int n = 0;

    LISTC_FOR_EACH(&list, res, link)
    {
        /* "lrl" is the master lrl itself - it is regenerated separately and
         * never appears as a "resource" directive in lrl text. */
        if (strcmp(res->name, "lrl") == 0)
            continue;

        const char *base = strrchr(res->filepath, '/');
        base = base ? base + 1 : res->filepath;

        char path[PATH_MAX];
        if (snprintf(path, sizeof(path), "%s/%s", dir, base) >= (int)sizeof(path)) {
            logmsg(LOGMSG_ERROR, "%s: dest path too long for resource %s\n", __func__, res->name);
            return -1;
        }

        FILE *in = fopen(res->filepath, "r");
        if (in == NULL) {
            logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, res->filepath, strerror(errno));
            return -1;
        }
        FILE *out = fopen(path, "w");
        if (out == NULL) {
            logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, path, strerror(errno));
            fclose(in);
            return -1;
        }

        /* raw byte copy - resources may be binary (.jar) files */
        // TODO: is this really needed? ( /bb/bin/comdb2translisten.jar )
        // does this need dumping?
        char buf[4096];
        size_t nr;
        int err = 0;
        while ((nr = fread(buf, 1, sizeof(buf), in)) > 0) {
            if (fwrite(buf, 1, nr, out) != nr) {
                logmsg(LOGMSG_ERROR, "%s:fwrite(\"%s\"):%s\n", __func__, path, strerror(errno));
                err = 1;
                break;
            }
        }
        if (!err && ferror(in)) {
            logmsg(LOGMSG_ERROR, "%s:fread(\"%s\"):%s\n", __func__, res->filepath, strerror(errno));
            err = 1;
        }
        fclose(in);
        if (fclose(out) != 0 && !err) {
            logmsg(LOGMSG_ERROR, "%s:fclose(\"%s\"):%s\n", __func__, path, strerror(errno));
            err = 1;
        }
        if (err)
            return -1;

        logmsg(LOGMSG_INFO, "%s copied resource %s -> %s\n", __func__, res->name, path);
        ++n;
    }
    return n;
}
