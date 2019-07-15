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

#ifndef INCLUDED_LISTC_H
#define INCLUDED_LISTC_H

/* LISTC.H   -   definition for list manipulation routines */
/* version 3 with count */

#include <stddef.h> /*THIS DEFINES offsetof() MACRO*/

#ifdef LIST_EMPTY
#undef LIST_EMPTY
#endif
#define LIST_EMPTY(listp) ((listp)->count == 0)

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

#define LISTC_T(type)                                                          \
    struct {                                                                   \
        type *top;                                                             \
        type *bot;                                                             \
        size_t diff;                                                           \
        int count;                                                             \
    }

#define LINKC_T(type)                                                          \
    struct {                                                                   \
        type *next;                                                            \
        type *prev;                                                            \
    }

#define list_init() ERROR_USING_LISTC
#define list_rfl() ERROR_USING_LISTC
#define list_rbl() ERROR_USING_LISTC
#define list_rtl() ERROR_USING_LISTC
#define list_abl() ERROR_USING_LISTC
#define list_atl() ERROR_USING_LISTC
#define list_ins() ERROR_USING_LISTC

#ifdef __cplusplus
extern "C" {
#endif

/* allocate a new list with offsetof() diff for link */
extern void *listc_new(int diff);

/* free list structure */
extern void listc_free(listc_t *l);

/* initialize listc_t struct.  using offsetof(), specify offset of linkc_t
 * struct */
extern void listc_init(void *list, int offset);

/* remove from list. returns item removed*/
extern void *listc_rfl(void *list, void *obj);

/* remove from bottom of list */
extern void *listc_rbl(void *list);

extern void *listc_add_before(void *list, void *obj, void *beforeobj);

/* remove from top of list */
extern void *listc_rtl(void *list);

/* add to bottom of list. returns item added */
extern void *listc_abl(void *list, void *obj);

/* add to top of list. returns item added */
extern void *listc_atl(void *list, void *obj);

extern void *listc_add_after(void *list, void *obj, void *afterobj);

/* number of elements in list */
extern int listc_size(void *list);

#define LISTC_FOR_EACH(listp, currentp, linkv)                                 \
    for ((currentp) = ((listp)->top); (currentp) != 0;                         \
         (currentp) = ((currentp)->linkv.next))

#define LISTC_FOR_EACH_REVERSE(listp, currentp, linkv)                         \
    for ((currentp) = ((listp)->bot); (currentp) != 0;                         \
         (currentp) = ((currentp)->linkv.prev))

#define LISTC_BOT(listp) ((listp)->bot)

#define LISTC_TOP(listp) ((listp)->top)

#define LISTC_NEXT(currentp, linkv) ((currentp)->linkv.next)

/* Iteration in which it is safe to remove the current element.
 * tmpp is an additional pointer of the same type as currentp,
 * to cache the next element */
#define LISTC_FOR_EACH_SAFE(listp, currentp, tmpp, linkv)                      \
    for ((currentp) = ((listp)->top),                                          \
        (tmpp) = (currentp) ? ((currentp)->linkv.next) : 0;                    \
         (currentp) != 0;                                                      \
         (currentp) = (tmpp), (tmpp) = (tmpp) ? ((tmpp)->linkv.next) : 0)

#ifdef __cplusplus
} /*extern "C"*/
#endif

#endif
