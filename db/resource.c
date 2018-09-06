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
    listc_init(&list, offsetof(struct resource, link));
    if (!newlrlname)
        return;

    if (lrlname) // free before assigning new one
        free(lrlname);

    char *mem = NULL;
#   if defined(_IBM_SOURCE)
    mem = malloc(PATH_MAX);
#   endif
    lrlname = realpath(newlrlname, mem);

    /* lrl file is always known as "lrl" */
    if (lrlname)
        addresource("lrl", lrlname);
#   if defined(_IBM_SOURCE)
    else
        free(mem);
#   endif
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

    /* if relpath is absolute then return it unaltered */
    if (relpath[0] == '/')
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
