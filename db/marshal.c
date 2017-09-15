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

/*resource marshaller */

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <strings.h>

#include <list.h>

#include <plhash.h>
#include "marshal.h"

#include <mem_uncategorized.h>
#include <mem_override.h>

#include <logmsg.h>

struct marshal_t {
    char *name;
    hash_t *h_ptrs;
    LISTC_T(struct marshal_itm) l_itms;
};

struct marshal_itm {
    /*vvv  start key*/
    void *ptr;
    /*^^^  end key*/
    void (*free)(void *);
    char *name;
    LINKC_T(struct marshal_itm) nxt;
};

/* new resource marshaller*/
marshal_t *new_marshal(char *name)
{
    marshal_t *new = (marshal_t *)malloc(sizeof(*new));
    new->h_ptrs = hash_init(sizeof(void *));
    listc_init(&new->l_itms, offsetof(struct marshal_itm, nxt));
    new->name = strdup(name);
    return new;
}

/*marshal a resource - returns ptr to passed in ptr. this is so you
  can  var=marshal_add(..., ptr) in one line */
void *marshal_add(marshal_t *mm, void *ptr, void (*free_mem_func)(void *))
{
    struct marshal_itm *itm;
    if (ptr == 0)
        return 0; /*dont add null ptr*/
    if (free_mem_func == 0) return 0; /*no free? why marshall.*/
    if (hash_find(mm->h_ptrs, &ptr) != 0) {
        logmsg(LOGMSG_ERROR, "marshal_add:error dup add %p in %s\n", ptr, mm->name);
        return ptr;
    }
    itm = (struct marshal_itm *)malloc(sizeof(*itm));
    itm->ptr = ptr;
    itm->free = free_mem_func;
    listc_atl(&mm->l_itms,
              itm); /*add as first item on list... first one to be freed*/
    hash_add(mm->h_ptrs, itm);
    /*printf("mm %s add itm %p with free %p\n",mm->name,itm,itm->free);*/
    return ptr;
}

/*internal delete item*/
static int del_itm(marshal_t *mm, struct marshal_itm *itm)
{
    /*printf("mm %s delete itm %p with free %p\n",mm->name,itm,itm->free);*/
    itm->free(itm->ptr); /*user provided func*/
    itm->free = 0;
    itm->ptr = 0;
    listc_rfl(&mm->l_itms, itm);
    hash_del(mm->h_ptrs, itm);
    free(itm);
    return 0;
}

/*delete individual item from marshaller*/
int marshal_del(marshal_t *mm, void *ptr)
{
    struct marshal_itm *itm;
    if (ptr == 0)
        return -1;                     /*no null ptrs*/
    itm = hash_find(mm->h_ptrs, &ptr); /*look up this ptr*/
    if (itm == 0) {
        logmsg(LOGMSG_ERROR, "marshal_del:failed to del %p from %s\n", ptr,
                mm->name);
        return -1; /*not fnd?*/
    }
    return del_itm(mm, itm);
}

/*malloc a buffer and add to marshal buffer*/
void *marshal_malloc(marshal_t *mm, size_t size)
{
    return marshal_add(mm, malloc(size), free);
}

/*calloc a buffer and add to marshal buffer*/
void *marshal_calloc(marshal_t *mm, size_t nelem, size_t size)
{
    return marshal_add(mm, calloc(nelem, size), free);
}

/*change pointer, but keep position in marshaller*/
void *marshal_chg(marshal_t *mm, void *orig, void *new)
{
    struct marshal_itm *itm;
    if (orig == 0 || new == 0)
        return new;
    if (orig == new)
        return new;
    itm = hash_find(mm->h_ptrs, &orig);
    if (itm == 0) {
        logmsg(LOGMSG_ERROR, "marshal_chg:failed to find orig %p from %s\n", orig,
                mm->name);
        return new; /*not fnd?*/
    }
    hash_del(mm->h_ptrs, itm);
    itm->ptr = new;
    hash_add(mm->h_ptrs, itm);
    return new;
}

/*reallocate a malloc'd buffer, maintain priority in marshaller*/
void *marshal_realloc(marshal_t *mm, void *orig, size_t size)
{
    void *new;
    new = realloc(orig, size);
    if (new == 0)
        return 0;
    if (orig != 0)
        return marshal_chg(mm, orig, new);
    else
        return marshal_add(mm, new, free);
}

/*marshal strdup */
char *marshal_strdup(marshal_t *mm, char *str)
{
    return (char *)marshal_add(mm, strdup(str), free);
}

/*marshal a string allocated with malloc/strdup/etc.*/
char *marshal_add_str(marshal_t *mm, char *str)
{
    return (char *)marshal_add(mm, str, free);
}

/*free all resources and free marshaller*/
/* this frees in reverse order of add.  last in, first freed*/
void release_marshal(marshal_t *mm)
{
    int rc;
    if (mm == 0)
        return; /*null passed in*/
    while (mm->l_itms.top) {
        rc = del_itm(mm, mm->l_itms.top);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "free_marshal:failed to free %s\n", mm->name);
            return;
        }
    }
    free(mm->name);
    mm->name = 0;
    free(mm);
}

/* this release_marshal and perror.  returns NULL...*/
void *release_marshal_perr(marshal_t *mm, char *msg)
{
    /*error return. frees current allocated stuff */
    logmsgperror(msg);
    release_marshal(mm);
    return 0;
}
