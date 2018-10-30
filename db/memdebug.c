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

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <dlfcn.h>
#ifdef __sun
#include <link.h>
#endif
#include <pthread.h>
#include <alloca.h>
#include <strings.h>

#include <plhash.h>
#include <list.h>
#include <walkback.h>
#include <lockmacro.h>
#include <logmsg.h>

#if 0
#include <dlmalloc.h>

/* enough for most... */
#define MAXSTACKDEPTH 200

/* this many frames are always the same (walkback context, etc.), so skip them */
#define SKIPFRAMES 4

static pthread_mutex_t lk;
static pthread_key_t threadk;

enum {
    FLAG_DEBUG_BLOCK = 1
};

static FILE *memdebug_out;

struct memblock;

struct thread_info {
    pthread_t tid;
    LISTC_T(struct memblock) blocks;
};

struct memblock {
    struct call *place_allocated;
    LINKC_T(struct memblock) alnk;   /* anchor in list of all blocks */
    LINKC_T(struct memblock) clnk;   /* anchor in list of caller's blocks */
    LINKC_T(struct memblock) tlnk;   /* anchor in thread's list of blocks */
    int blocksize;
    int flags;
    struct thread_info *ti;
    double align;
    unsigned char mem[1]; 
};

struct call {
    int total_allocated;
    int nframes;
    LISTC_T(struct memblock) blocks;
    intptr_t ip[1];
};

struct symbol {
    intptr_t addr;
    int sz;
    char *sym;
};

void* dl_malloc(size_t sz) {
    void *out;
    LOCK(&lk) {
        out = dlmalloc(sz);
    } UNLOCK(&lk);
    return out;
}

void* dl_calloc(size_t nelem, size_t elemsz) {
    void *out;
    LOCK(&lk) {
        out = dlcalloc(nelem, elemsz);
    } UNLOCK(&lk);
    return out;
}

void* dl_realloc(void *p, size_t sz) {
    void *out;
    LOCK(&lk) {
        out = dlrealloc(p, sz);
    } UNLOCK(&lk);
    return out;
}

void dl_free(void *p) {
    LOCK(&lk) {
        dlfree(p);
    } UNLOCK(&lk);
}

/* all places that allocate memory */
static hash_t *calls;
static int in_init = 1;
static struct symbol *symtab = NULL;
static int numsyms = 0;

void memdebug_dump_callers(FILE *f, int skip_balanced);

LISTC_T(struct memblock) blocks;

static u_int call_hashfunc(void *key, int len) {
    struct call *c = (struct call*) key;
    int i;
    u_int h = 0;
    for (i = 0; i < c->nframes; i++)
        h ^= (u_int) c->ip[i];

#if 0
    printf("hash: ");
    for (i = 0; i < c->nframes; i++)
        printf("%p ", c->ip[i]);
    printf(" -> %u\n", h);
#endif

    return h;
}

static u_int sym_hashfunc(void *key, int len) {
    struct symbol *s = (struct symbol*) key;
    return (u_int) s->addr;
}

static int sym_cmpfunc(void *key1, void *key2, int len) {
    struct symbol *s1, *s2;
    s1 = (struct symbol *) key1;
    s2 = (struct symbol *) key2;

    return (s1->addr - s2->addr);
}

static int sym_qsort_cmpfunc(const void *key1, const void *key2) {
    struct symbol *s1, *s2;
    s1 = (struct symbol *) key1;
    s2 = (struct symbol *) key2;

    return (s1->addr - s2->addr);
}

static int call_cmpfunc(void *key1, void *key2, int len) {
    struct call *c1, *c2;
    int i;
    c1 = (struct call*) key1;
    c2 = (struct call*) key2;

    if (c1->nframes != c2->nframes)
        return -1;

    for (i = 0; i < c1->nframes; i++)
        if (c1->ip[i] != c2->ip[i])
            return -1;

    return 0;
}

static int sym_bsearch_cmpfunc(const void *p1, const void *p2) {
    struct symbol *key, *cmp; 

    key = (struct symbol*) p1;
    cmp = (struct symbol*) p2;

    if (key == NULL || cmp == NULL)
        return -1;

    /* if the address is within function bounds, we found it. */
    if (key->addr > cmp->addr && key->addr < (cmp->addr + cmp->sz))
        return 0;
    return key->addr - cmp->addr;
}

static void chomp(char *s) {
    while (*s) {
        if (*s == '\n') {
            *s = 0;
            break;
        }
        s++;
    }
}

/* this gets run pre-init, so it can use the default system allocators */
static void init_symtab(void) {
    FILE *f;
    char cmd[1024];
    char *prog;
    char *symfile;
    struct symbol *s;
    intptr_t addr;
    int sz;
    int allocsyms = 0;

    /* There's no good way to get at this that I know of.  get_progname_by_pid
       is the best I could fine, but it doesn't work for longer paths. */
    symfile = getenv("MEMDEBUG_SYMBOLS");
    if (symfile == NULL) {
        prog = getenv("MEMDEBUG_TASKNAME");
        /* kludgeit */
        if (prog == NULL)
            prog = "/proc/self/exe";

        /* on sun... */
#ifdef __sun
        snprintf(cmd, sizeof(cmd), "nm -ev '%s' | grep FUNC | awk -F\\| '{ print $2,$3,$8 }'", prog);
#else
        snprintf(cmd, sizeof(cmd), "nm -dPp '%s' | grep ' [Tt] ' | egrep -v '\\.bf|\\.ef|\\.eb|\\.bb' | awk '{ print $3,$4,$1 }'", prog);
#endif
        f = popen(cmd, "r");
    }
    else {
        f = fopen(symfile, "r");
    }

    if (f == NULL) {
        logmsg(LOGMSG_WARN, "Can't open symbol file\n");
    }

    while (fgets(cmd, sizeof(cmd), f)) {
        char *tok;
        tok = strtok(cmd, " \t\n");
        if (tok == NULL)
            continue;
        addr = (intptr_t) strtoul(tok, NULL, 10);
        tok = strtok(NULL, " \t\n");
        if (tok == NULL)
            continue;
        sz = (int) strtol(tok, NULL, 10);
        tok = strtok(NULL, " \t\t");
        if (tok == NULL)
            continue;
        if (numsyms >= allocsyms) {
            if (symtab)
                symtab = dl_realloc(symtab, sizeof(struct symbol) * (numsyms + 1000));
            else
                symtab = dl_malloc(sizeof(struct symbol) * (numsyms + 1000));
            allocsyms = numsyms + 1000;
        }
        symtab[numsyms].addr = addr;
        symtab[numsyms].sz = sz;
        symtab[numsyms].sym = strdup(tok);
        chomp(symtab[numsyms].sym);
        numsyms++;
    }
    qsort(symtab, numsyms, sizeof(struct symbol), sym_qsort_cmpfunc);
}

/* find symbol with the greatest address <= addr.
   if the address is within that symbol's size, we are in
   that function.  otherwise we are in limbo */
static struct symbol *findsym(intptr_t addr) {
    struct symbol key;

    if (numsyms == 0)
        return NULL;

    key.addr = addr;
    key.sz = -1;

    return (struct symbol*) bsearch(&key, symtab, numsyms, 
            sizeof(struct symbol), sym_bsearch_cmpfunc);
}

static void dump_caller(FILE *f, struct call *c);

/* return structure corresponding to the current call stack (minus
   frames used by memdebug itself) */
static struct call* get_call() {
    int rc;
    unsigned nframes;
    int i;
    char *symname;
    struct symbol *s;
    struct call *tmp;
    struct call *c;
    tmp = alloca(offsetof(struct call, ip) * MAXSTACKDEPTH);

#ifdef _LINUX_SOURCE
    {
        rc = -1;
    }
#else
    rc = stack_pc_getlist(NULL, (void**) tmp->ip, MAXSTACKDEPTH,
            (unsigned*) &tmp->nframes);
#endif
    if (rc) return NULL; /* shouldn't happen */

    /* see if this call already exists */
    c = hash_find(calls, tmp);
    if (c == NULL) {
        /* doesn't exist, create and add to hash */
        c = dl_malloc(offsetof(struct call, ip) * tmp->nframes);
        c->total_allocated = 0;
        c->nframes = tmp->nframes;
        for (i = 0; i < tmp->nframes; i++)
            c->ip[i] = tmp->ip[i];
        listc_init(&c->blocks, offsetof(struct memblock, clnk));
        hash_add(calls, c);
    }

    return c;
}

static void thread_done(void *p) {
    struct thread_info *info = (struct thread_info*) p;
    struct memblock *b;

#if 0
    fprintf(memdebug_out, "thread %d exiting\n", info->tid);
    LOCK(&lk) {
        LISTC_FOR_EACH(&info->blocks, b, tlnk) {
            fprintf(memdebug_out, " 0x%p %d\n", b->mem, b->blocksize);
            dump_caller(memdebug_out, b->place_allocated);
        }
    } UNLOCK(&lk);
#endif
}

static struct thread_info *get_thread_info(void) {
    struct thread_info *ti;

    ti = pthread_getspecific(threadk);
    if (ti == NULL) {
        ti = dl_malloc(sizeof(struct thread_info));
        ti->tid = pthread_self();
        listc_init(&ti->blocks, offsetof(struct memblock, tlnk));
        pthread_setspecific(threadk, ti);
    }
    return ti;
}

void dump_callers_at_exit(void) {
    memdebug_dump_callers(memdebug_out, 1);
}

static void init() {
    static int once = 1;
    if (once) {
        once = 0;
        pthread_mutexattr_t mattr;
        pthread_mutexattr_init(&mattr);
        pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_RECURSIVE);
        pthread_mutex_init(&lk, &mattr);
        pthread_key_create(&threadk, thread_done);
        calls = hash_setalloc_init_user(call_hashfunc, call_cmpfunc, dl_malloc, dl_free, 0, sizeof(int));
        listc_init(&blocks, offsetof(struct memblock, alnk));
        init_symtab();
        in_init = 0;
        /* if the debug variable is not set, the malloc/etc. code continues to call real malloc */

        memdebug_out = fopen("memdebug.out", "w");
        setbuf(memdebug_out, NULL);

        atexit(dump_callers_at_exit);
    }
}

void *malloc(size_t size) {
    struct call *c;
    struct memblock *b;
    struct thread_info *ti;

    init();
    if (in_init)
        return dl_malloc(size);

    ti = get_thread_info();

    b = dl_malloc(offsetof(struct memblock, mem) + size);
    b->blocksize = size;
    b->flags = 0;
    b->ti = ti;
    LOCK(&lk) {
        c = get_call();
        b->place_allocated = c;
        c->total_allocated += size;
        listc_abl(&c->blocks, b);
        listc_abl(&blocks, b);
        listc_abl(&ti->blocks, b);
    } UNLOCK(&lk);
    return b->mem;
}

void *calloc(size_t nelem, size_t elsize) {
    struct call *c;
    struct memblock *b;
    int size;
    struct thread_info *ti;

    init();
    if (in_init)
        return dl_calloc(nelem, elsize);

    ti = get_thread_info();

    size = nelem * elsize;
    b = dl_malloc(offsetof(struct memblock, mem) + size);
    b->flags = 0;
    b->blocksize = size;
    LOCK(&lk) {
        c = get_call();
        b->ti = ti;
        b->place_allocated = c;
        c->total_allocated += size;
        listc_abl(&c->blocks, b);
        listc_abl(&blocks, b);
        listc_abl(&ti->blocks, b);
    } UNLOCK(&lk);
    bzero(b->mem, size);
    return b->mem;
}

void free(void *ptr) {
    struct call *c;
    struct memblock *b;
    struct thread_info *ti;

    init();

    if (ptr == NULL)
        return;

    if (in_init) {
        dl_free(ptr);
        return;
    }
    LOCK(&lk) {
        /* ptr is a pointer to the user portion of the memblock, map back */
        b = (struct memblock *) ((intptr_t) ptr - offsetof(struct memblock, mem));
        c = b->place_allocated;
        c->total_allocated -= b->blocksize;
        listc_rfl(&b->ti->blocks, b);
        listc_rfl(&c->blocks, b);
        listc_rfl(&blocks, b);
    } UNLOCK(&lk);
    dl_free(b);
}

void *realloc(void *ptr, size_t size) {
    struct call *c;
    struct memblock *b;
    struct thread_info *ti;
    size_t was_size = 0;

    /* record new place we allocated from */
    ti = get_thread_info();

    init();
    if (in_init)
        return dl_realloc(ptr, size);


    /* this part is identical to free */
    LOCK(&lk) {
        if (ptr != NULL) {
            /* get back to memblock */
            b = (struct memblock *) ((intptr_t) ptr - offsetof(struct memblock, mem));
            c = b->place_allocated;
            c->total_allocated -= b->blocksize;
            listc_rfl(&c->blocks, b);
            listc_rfl(&blocks, b);
            listc_rfl(&b->ti->blocks, b);
            was_size = b->blocksize;
        }
        else
            b = NULL;

        /* do the real realloc (include the memblock portion). done under
           lock to prevent funny math */
        b = dl_realloc(b, offsetof(struct memblock, mem) + size);

        /* readjust block size */
        b->blocksize = size;

        c = get_call();
        b->place_allocated = c;
        c->total_allocated += size;
        b->ti = ti;
        listc_abl(&c->blocks, b);
        listc_abl(&blocks, b);
        listc_abl(&ti->blocks, b);
    } UNLOCK(&lk);

    return b->mem;
}

char *strdup(const char *s) {
    int sz;
    struct call *c;
    struct memblock *b;
    struct thread_info *ti;

    sz = strlen(s) + 1;

    if (in_init) {
        char *out;
        out = dl_malloc(sz);
        strcpy(out, s);
        return out;
    }

    init();

    ti = get_thread_info();

    /* the rest is pretty similar to malloc */
    b = dl_malloc(offsetof(struct memblock, mem) + sz);
    b->blocksize = sz;
    LOCK (&lk) {
        c = get_call();
        b->place_allocated = c;
        c->total_allocated += sz;
        b->ti = ti;

        listc_abl(&c->blocks, b);
        listc_abl(&blocks, b);
        listc_abl(&ti->blocks, b);

    } UNLOCK(&lk);
    strcpy((char*) b->mem, s);
    return (char*) b->mem;
}

static void dump_caller(FILE *f, struct call *c) {
    int i;
    struct symbol *s;

    for (i = SKIPFRAMES; i < c->nframes; i++) {
        s = findsym(c->ip[i]);
        if (s)
            logmsgf(LOGMSG_USER, f, "  [%2d] %p %s+%d\n", i-SKIPFRAMES, c->ip[i], s->sym, (intptr_t) c->ip[i] - s->addr);
        else
            logmsgf(LOGMSG_USER, f, "  [%2d] %p ????\n", i-SKIPFRAMES, c->ip[i]);
    }
}

struct dumpconfig {
    FILE *f;
    int skip_balanced;
};

static int dump_call(void *obj, void *arg) {
    struct call *c = (struct call *) obj;
    struct dumpconfig *d = (struct dumpconfig*) arg;

    if (!d->skip_balanced || c->total_allocated != 0) {
        logmsgf(LOGMSG_USER, d->f, "%d bytes in %d blocks\n", c->total_allocated, c->blocks.count);
        dump_caller(d->f, c);
    }

    return 0;
}


/* The "API" - dump various memory stats */

void memdebug_dump_callers(FILE *f, int skip_balanced) {
    struct dumpconfig d;
    d.f = f;
    d.skip_balanced = skip_balanced;

    if (calls == NULL)
        return;
    LOCK(&lk) {
        hash_for(calls, dump_call, &d);
    } UNLOCK(&lk);
}

void memdebug_dump_blocks(FILE *f) {
    struct memblock *b;
    if (calls == NULL)
        return;
    LOCK(&lk) {
        LISTC_FOR_EACH(&blocks, b, alnk) {
            if (b->flags & FLAG_DEBUG_BLOCK) {
                logmsgf(LOGMSG_USER, f, "%s\n", b->mem);
            }
            else {
            logmsgf(LOGMSG_USER, f, "block at %p size %d {\n", b->mem, b->blocksize);
            dump_caller(f, b->place_allocated);
            logmsgf(LOGMSG_USER, f, "}\n");
        }
        }
    } UNLOCK(&lk);
}

void memdebug_stats(void) {
    struct mallinfo m;
    LOCK(&lk) {
        m = dlmallinfo();
    } UNLOCK(&lk);
    logmsg(LOGMSG_USER, "%d/%d bytes allocated\n", (int) m.uordblks, (int) m.usmblks);
}

#ifdef TEST_MEMDEBUG
int main(int argc, char *argv[]) {
    void *p[6];
    int i;
    char *s;

    printf("allocated memory at start:\n");
    memdebug_dump_callers(stdout, 1);
    printf("---------------------\n");

    p[0] = malloc(1024);
    printf("allocated memory after malloc(1024):\n");
    memdebug_dump_callers(stdout, 1);
    printf("---------------------\n");

    for (i = 1; i < 6; i++) {
        printf("allocated memory after calloc %d\n", i);
        p[i] = calloc(1, i*10);
        memdebug_dump_callers(stdout, 1);
        printf("---------------------\n");
    }

    printf("list of alloced blocks:\n");
    memdebug_dump_blocks(stdout);
    printf("---------------------\n");

    for (i = 1; i < 6; i++) {
        free(p[i]);
        printf("allocated memory after free %d\n", i);
        memdebug_dump_callers(stdout, 1);
        printf("---------------------\n");
    }

    s = strdup("hello!!!");
    printf("allocated memory after strdup\n");
    memdebug_dump_callers(stdout, 1);
    printf("---------------------\n");
    free(s);
    printf("allocated memory after free of strdup\n");
    memdebug_dump_callers(stdout, 1);
    printf("---------------------\n");

    p[0] = realloc(p[0], 2048);
    printf("allocated memory after realloc\n");
    memdebug_dump_callers(stdout, 1);
    printf("---------------------\n");
    free(p[0]);
    printf("allocated memory after free of strdup\n");
    memdebug_dump_callers(stdout, 1);
    printf("---------------------\n");
}
#endif

#else

void memdebug_dump_callers(FILE *f, int skip_balanced) {}
void memdebug_dump_blocks(FILE *f) {}
void memdebug_stats() {}

#endif
