#include <stackutil.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <plhash_glue.h>
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <sys_wrap.h>
#include "dbinc/maxstackframes.h"
#include "strbuf.h"

#include <stackutil.h>

struct stack {
    int id;
    char *type;
    int64_t hits;
    int nframes;
    void *frames[MAX_STACK_FRAMES];
};

static hash_t *stackhash = NULL;
static struct stack **stacks = NULL;
static int stackid = 0;
#ifdef __GLIBC__
static int stackalloc = 0;
static pthread_once_t stacksonce = PTHREAD_ONCE_INIT;
#endif
static pthread_mutex_t stacklk = PTHREAD_MUTEX_INITIALIZER;

static void once(void)
{
    struct stack *s;
    stackhash = hash_init_o(offsetof(struct stack, frames), sizeof(s->frames));
}

#ifdef __GLIBC__
extern int backtrace(void **, int);
extern char **backtrace_symbols(void *const *, int);
#endif

int stackutil_get_stack_id(const char *type)
{
    int id = -1;
#ifdef __GLIBC__
    struct stack s = {0};
    Pthread_once(&stacksonce, once);

    if (stackhash == NULL)
        return -1;

    s.nframes = backtrace(s.frames, MAX_STACK_FRAMES);

    Pthread_mutex_lock(&stacklk);
    struct stack *have_stack = hash_find(stackhash, s.frames);
    if (!have_stack) {
        struct stack *newstack = malloc(sizeof(struct stack));
        *newstack = s;
        hash_add(stackhash, newstack);
        if (stackid == stackalloc) {
            stackalloc = stackalloc * 2 + 16;
            stacks = realloc(stacks, sizeof(struct stack *) * stackalloc);
        }

        stacks[stackid] = newstack;
        newstack->type = strdup(type);
        newstack->id = stackid;
        newstack->hits = 1;
        id = stackid++;
    } else {
        have_stack->hits++;
        id = have_stack->id;
    }
    Pthread_mutex_unlock(&stacklk);
#endif
    return id;
}

int stackutil_get_stack(int id, char **type, int *nframes, void *frames[MAX_STACK_FRAMES], int64_t *hits)
{
    if (type)
        *type = NULL;
#ifndef __GLIBC__
    return -1;
#endif
    Pthread_mutex_lock(&stacklk);
    if (id < 0 || id >= stackid) {
        Pthread_mutex_unlock(&stacklk);
        return -1;
    }
    if (nframes)
        *nframes = stacks[id]->nframes;
    for (int i = 0; i < stacks[id]->nframes; i++) {
        frames[i] = stacks[id]->frames[i];
    }
    if (hits)
        *hits = stacks[id]->hits;
    if (type)
        *type = strdup(stacks[id]->type);
    Pthread_mutex_unlock(&stacklk);
    return 0;
}

char *stackutil_get_stack_str(int id, char **type, int *in_nframes, int64_t *hits)
{
    char *str = NULL;
#ifdef __GLIBC__
    int inframes;
    if (type)
        *type = NULL;
    if (in_nframes)
        *in_nframes = 0;
    void *frames[MAX_STACK_FRAMES];
    if (stackutil_get_stack(id, type, &inframes, frames, hits) == 0) {
        if (in_nframes)
            *in_nframes = inframes;
        char **strings = NULL;
        strings = backtrace_symbols(frames, inframes);
        strbuf *b = strbuf_new();
        for (int j = 0, pr = 0; j < inframes; j++) {
            char *p = strchr(strings[j], '('), *q = strchr(strings[j], '+');
            if (p && q) {
                (*p) = (*q) = '\0';
                p++;
                q--;
                if ((q - p) > 0) {
                    if (pr)
                        strbuf_append(b, " ");
                    strbuf_appendf(b, "%s", p);
                    pr = 1;
                }
            }
        }
        str = strbuf_disown(b);
        free(strings);
    }
#endif
    return str;
}

int stackutil_get_num_stacks(void)
{
    Pthread_mutex_lock(&stacklk);
    int id = stackid;
    Pthread_mutex_unlock(&stacklk);
    return id;
}
