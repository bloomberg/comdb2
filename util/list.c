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

/*______ LIST ROUTINES w/ counts ______*/
#include <stdio.h>
#include <stdlib.h>

#include <bb_inttypes.h>

#ifndef BUILDING_TOOLS
#include "mem_util.h"
#include "mem_override.h"
#endif
#include "logmsg.h"

typedef struct linkc_t {
    void *next;
    void *prev;
} linkc_t;

typedef struct {
    void *top;
    void *bot;
    size_t diff;
    int count;
} listc_t;

void listc_init(listc_t *l, size_t diff);
listc_t *listc_new(size_t diff);
void *listc_atl(listc_t *l, void *obj);
void *listc_abl(listc_t *l, void *obj);
void *listc_rfl(listc_t *l, void *obj);
void *listc_rtl(listc_t *l);
void *listc_rbl(listc_t *l);
int listc_size(listc_t *l);

#ifdef DEBUG_LISTS
/* Return 1 if the element is present, 0 if not */
static inline int listc_is_present(listc_t *l, void *inobj)
{
    char *obj;

    for (obj = (char *)l->top; obj != 0;) {
        if (obj == inobj)
            return 1;
        linkc_t *lnk = (linkc_t *)((char *)obj + l->diff);
        obj = (char *)lnk->next;
    }

    return 0;
}

static inline void listc_verify_there(listc_t *l, void *obj)
{
    if (!listc_is_present(l, obj)) {
        logmsg(LOGMSG_FATAL, "Corrupt list call, obj not present.\n");
        abort();
    }
}

static inline void listc_verify_not_there(listc_t *l, void *obj)
{
    if (listc_is_present(l, obj)) {
        logmsg(LOGMSG_FATAL, "Corrupt list call, obj is present.\n");
        abort();
    }
}

static inline void listc_verify_count(listc_t *l)
{
    int cnt = 0, i, bt_count;
    char *obj;

    if (NULL == l)
        return;

    /* Make sure the number of elements is correct */
    for (obj = (char *)l->top; obj != 0;) {
        linkc_t *lnk = (linkc_t *)((char *)obj + l->diff);
        obj = (char *)lnk->next;
        cnt++;
    }

    /* Verify count */
    if (cnt != l->count) {
        logmsg(LOGMSG_FATAL, "Corrupt list count.\n");

        /* I should be able to look through the list */
        abort();
    }
}

#endif

void listc_init(listc_t *l, size_t diff)
{
    l->top = 0;
    l->bot = 0;
    l->count = 0;
    l->diff = diff;
}

listc_t *listc_new(size_t diff)
{
    listc_t *l;
    l = (listc_t *)malloc(sizeof(*l));
    if (l == 0)
        return 0;
    l->top = 0;
    l->bot = 0;
    l->count = 0;
    l->diff = diff;
    return l;
}

void listc_free(listc_t *l) { free(l); }

void *listc_atl(listc_t *l, void *obj)
{
    linkc_t *it = (linkc_t *)((intptr_t)obj + l->diff);
#if DEBUG_LISTS
    listc_verify_not_there(l, obj);
#endif
    /*if (it->next!=0 && it->prev!=0) { abort(); }*/
    if (!l->top) /* empty list */
    {
        l->top = l->bot = obj;
        it->next = it->prev = 0;
    } else {
        it->next = l->top;
        it->prev = 0;
        ((linkc_t *)((intptr_t)l->top + l->diff))->prev = obj;
        l->top = obj;
    }
    l->count++;
#if DEBUG_LISTS
    listc_verify_count(l);
#endif
    return obj;
}

void *listc_abl(listc_t *l, void *obj)
{
    linkc_t *it = (linkc_t *)((intptr_t)obj + l->diff);
#if DEBUG_LISTS
    listc_verify_not_there(l, obj);
#endif
    /*if (it->next!=0 && it->prev!=0) { abort(); }*/
    if (!l->bot) /* empty list */
    {
        l->top = l->bot = obj;
        it->next = it->prev = 0;
    } else {
        it->next = 0;
        it->prev = l->bot;
        ((linkc_t *)((intptr_t)l->bot + l->diff))->next = obj;
        l->bot = obj;
    }
    l->count++;
#if DEBUG_LISTS
    listc_verify_count(l);
#endif
    return obj;
}

void *listc_add_after(listc_t *l, void *obj, void *afterobj)
{
    linkc_t *itins, *itaft;
#if DEBUG_LISTS
    listc_verify_not_there(l, obj);
#endif
    itins = (linkc_t *)((intptr_t)obj + l->diff);
    itaft = (linkc_t *)((intptr_t)afterobj + l->diff);
    itins->next = itaft->next;
    itins->prev = afterobj;
    if (itaft->next == 0) {
        /* bottom of list */
        if (l->bot != afterobj) {
            logmsg(LOGMSG_ERROR, "WARNING: ADD_AFTER %p WITH %p NOT IN LIST %p\n",
                    obj, afterobj, l);
            return 0;
        }
        l->bot = obj;
    } else {
        /* tack on to next */
        linkc_t *itnext = (linkc_t *)((intptr_t)itaft->next + l->diff);
        itnext->prev = obj;
    }
    itaft->next = obj;
    l->count++;
#if DEBUG_LISTS
    listc_verify_count(l);
#endif
    return obj;
}

void *listc_add_before(listc_t *l, void *obj, void *beforeobj)
{
    linkc_t *itins, *itb4;
#if DEBUG_LISTS
    listc_verify_not_there(l, obj);
#endif
    itins = (linkc_t *)((intptr_t)obj + l->diff);
    itb4 = (linkc_t *)((intptr_t)beforeobj + l->diff);
    itins->next = beforeobj;
    itins->prev = itb4->prev;
    if (itb4->prev == 0) {
        /* top of list */
        if (l->top != beforeobj) {
            logmsg(LOGMSG_ERROR, "WARNING: ADD_BEFORE %p WITH %p NOT IN LIST %p\n",
                    obj, beforeobj, l);
            return 0;
        }
        l->top = obj;
    } else {
        /* tack on to prev */
        linkc_t *itprev = (linkc_t *)((intptr_t)itb4->prev + l->diff);
        itprev->next = obj;
    }
    itb4->prev = obj;
    l->count++;
#if DEBUG_LISTS
    listc_verify_count(l);
#endif
    return obj;
}

void *listc_rfl(listc_t *l, void *obj)
{
    linkc_t *it = (linkc_t *)((intptr_t)obj + l->diff);
#if DEBUG_LISTS
    listc_verify_there(l, obj);
#endif
    if (l->top == l->bot) /* 1 entry in list */
    {
        if (l->top != obj) {
            logmsg(LOGMSG_ERROR, "WARNING: REMOVED WRONG ITEM %p FROM LIST %p\n",
                    obj, l);
            abort();
        }
        l->top = l->bot = 0;
    } else if (l->top == obj) /* top of list */
    {
        l->top = it->next;
        ((linkc_t *)((intptr_t)l->top + l->diff))->prev = 0;
    } else if (l->bot == obj) /* bottom of list */
    {
        l->bot = it->prev;
        ((linkc_t *)((intptr_t)l->bot + l->diff))->next = 0;
    } else /* middle of list */
    {
        ((linkc_t *)((intptr_t)it->prev + l->diff))->next = it->next;
        ((linkc_t *)((intptr_t)it->next + l->diff))->prev = it->prev;
    }
    it->next = it->prev = 0;
    l->count--;
#if DEBUG_LISTS
    listc_verify_count(l);
#endif
    return obj;
}

void *listc_rtl(listc_t *l)
{
    if (l->top == 0)
        return 0;
    return listc_rfl(l, l->top);
}

void *listc_rbl(listc_t *l)
{
    if (l->bot == 0)
        return 0;
    return listc_rfl(l, l->bot);
}

int listc_size(listc_t *l)
{
#if DEBUG_LISTS
    listc_verify_count(l);
#endif
    return l->count;
}
