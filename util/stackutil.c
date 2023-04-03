#include <stackutil.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <plhash.h>
#include <stdint.h>
#include <inttypes.h>
#include <stddef.h>
#include <locks_wrap.h>
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
static int stackalloc = 0;
static pthread_mutex_t stacklk = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t stacksonce = PTHREAD_ONCE_INIT;

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
    struct stack s = {0};
    int id = -1;

#ifdef __GLIBC__
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
    *type = NULL;
#ifndef __GLIBC__
    return -1;
#endif
    Pthread_mutex_lock(&stacklk);
    if (id < 0 || id >= stackid) {
        Pthread_mutex_unlock(&stacklk);
        return -1;
    }
    *nframes = stacks[id]->nframes;
    for (int i = 0; i < stacks[id]->nframes; i++) {
        frames[i] = stacks[id]->frames[i];
    }
    *hits = stacks[id]->hits;
    *type = strdup(stacks[id]->type);
    Pthread_mutex_unlock(&stacklk);
    return 0;
}

char *stackutil_get_stack_str(int id, char **type, int *nframes, int64_t *hits)
{
    void *frames[MAX_STACK_FRAMES];
    char *str = NULL;
    *type = NULL;

#ifdef __GLIBC__
    if (stackutil_get_stack(id, type, nframes, frames, hits) == 0) {
        char **strings = NULL;
        strings = backtrace_symbols(frames, *nframes);
        strbuf *b = strbuf_new();
        for (int j = 0, pr = 0; j < *nframes; j++) {
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
