#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <sys/queue.h>

#define UNW_LOCAL_ONLY
#include <libunwind.h>

#include <locks_wrap.h>
#include <plhash.h>

#include "stackutil.h"


struct stack {
    int64_t hits;
    unw_context_t context;
    char *names[MAXFRAMES];
    unw_word_t frames[MAXFRAMES];
    int id;
    int nframes;
};

static hash_t *stackhash;
static struct stack **stacks;
static int stackid = 0;
static int stackalloc = 0;
static pthread_mutex_t stacklk = PTHREAD_MUTEX_INITIALIZER;
static pthread_once_t stacksonce = PTHREAD_ONCE_INIT;


static void once(void) {
    struct stack *s;
    stackhash = hash_init_o(offsetof(struct stack, frames), sizeof(s->frames));
}

int stackutil_get_stack_id(void) {
    unw_context_t context;
    unw_cursor_t c;
    int rc;
    int id;
    struct stack s = {0};

    Pthread_once(&stacksonce, once);

    if (stackhash == NULL)
        return -1;

    rc = unw_getcontext(&context);
    if (rc)
        return -1;

    s.context = context;
    rc = unw_init_local(&c, &context);
    if (rc)
        return -1;

    while ((rc = unw_step(&c) > 0)) {
        rc = unw_get_reg(&c, UNW_REG_IP, &s.frames[s.nframes]);
        if (rc)
            break;
        s.nframes++;

        if (s.nframes == MAXFRAMES)
            break;
    }
    if (rc < 0)
        return -1;

    Pthread_mutex_lock(&stacklk);
    struct stack *have_stack = hash_find(stackhash, s.frames);
    if (have_stack == NULL) {
        rc = unw_init_local(&c, &context);
        if (rc) {
            Pthread_mutex_unlock(&stacklk);
            return -1;
        }

        char name[100];
        int i = 0;
        while (unw_step(&c) > 0) { 
            unw_word_t off;
            if (unw_get_proc_name(&c, name, sizeof(name), &off) != 0) {
                sprintf(name, "%"PRIx64, (uint64_t) s.frames[s.nframes]);
            }
            s.names[i++] = strdup(name);
        }

        struct stack *newstack = malloc(sizeof(struct stack));
        *newstack = s;
        newstack->hits = 1;
        hash_add(stackhash, newstack);
        if (stackid == stackalloc) {
            stackalloc = stackalloc * 2 + 16;
            stacks = realloc(stacks, sizeof(struct stack*) * stackalloc);
        }

        stacks[stackid] = newstack;
        newstack->id = stackid;
        id = stackid++;
    }
    else {
        have_stack->hits++;
        id = have_stack->id;
    }
    Pthread_mutex_unlock(&stacklk);

    return id;
}

int stackutil_get_stack_description(int id, char *frames[MAXFRAMES], int64_t *hits) {
    struct stack *s;
    Pthread_mutex_lock(&stacklk);
    if (id < 0 || id >= stackid) {
        Pthread_mutex_unlock(&stacklk);
        return -1;
    }
    s = stacks[id];
    Pthread_mutex_unlock(&stacklk);

    for (int i = 0; i < s->nframes; i++)
        frames[i] = s->names[i];
    if (s->nframes < MAXFRAMES)
        frames[s->nframes] = NULL;
    *hits = s->hits;
    return s->nframes;
}

int stackutil_get_num_stacks(void) {
    Pthread_mutex_lock(&stacklk);
    int id =  stackid;
    Pthread_mutex_unlock(&stacklk);
    return id;
}
