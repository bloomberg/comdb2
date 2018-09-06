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

#include <walkback.h>

#include "plhash.h"
#include "list.h"
#include "mem_util.h"
#include "mem_override.h"
#include "thread_util.h"

#include "thdpool.h"
#include "cheapstack.h"
#include <inttypes.h>

#define MAX_RESOURCE_TYPE 255
#define MAXSTACKDEPTH 64

int thread_debug = 0;
int dump_resources_on_thread_exit = 1;

static pthread_key_t thread_util_key;
static void (*describe_func[MAX_RESOURCE_TYPE])(void *);

struct thread_resource {
    int type;
    void *resource;
    LINKC_T(struct thread_resource) lnk;
    unsigned int nframes;
    void *stack[MAXSTACKDEPTH];
};

struct thread_info {
    pthread_t tid;
    arch_tid archtid;
    char *name;
    LISTC_T(struct thread_resource) resource_list;
    hash_t *resource_hash;
};

static void thread_util_donework_int(struct thread_info *info);

void thread_started(char *name)
{
    struct thread_info *info;

    info = pthread_getspecific(thread_util_key);
    if (info) {
        fprintf(stderr, "thread_started called twice\n");
        cheap_stack_trace();
        return;
    }

    info = malloc(sizeof(struct thread_info));
    info->tid = pthread_self();
    info->archtid = getarchtid();
    pthread_setspecific(thread_util_key, info);
    listc_init(&info->resource_list, offsetof(struct thread_resource, lnk));
    info->resource_hash =
        hash_init_o(offsetof(struct thread_resource, resource), sizeof(void *));
    info->name = strdup(name);
    if (thread_debug)
        printf("thd: started %s tid %" PRIx64 " archtid %u 0x%p\n", name,
               info->tid, info->archtid, info);
}

void thread_add_resource(int type, void *resource)
{
    struct thread_info *info;
    struct thread_resource *r;
    int rc;

    info = pthread_getspecific(thread_util_key);
    if (!info) {
        printf("thread_started not called\n");
        return;
    }

    r = malloc(sizeof(struct thread_resource));
    r->type = type;
    r->resource = resource;
    listc_abl(&info->resource_list, r);
    hash_add(info->resource_hash, r);

    r->nframes = 0;
    rc = stack_pc_getlist(NULL, r->stack, MAXSTACKDEPTH, &r->nframes);
    if (rc)
        printf("stack_pc_getlist rc %d\n", rc);
}

void thread_remove_resource(void *resource, void (*freefunc)(void *))
{
    struct thread_info *info;
    struct thread_resource *r;

    info = pthread_getspecific(thread_util_key);
    if (!info) {
        printf("thread_started not called\n");
        return;
    }
    r = hash_find(info->resource_hash, &resource);
    if (r == NULL) {
        printf("resource 0x%p not found, thread %" PRIu64 " archtid %u\n",
               resource, info->tid, info->archtid);
        return;
    }
    listc_rfl(&info->resource_list, r);
    hash_del(info->resource_hash, r);
    if (freefunc)
        freefunc(r->resource);
    free(r);
}

static void thread_ended(void *p)
{
    struct thread_info *info = p;
    struct thread_resource *r;

    if (thread_debug)
        printf("thd: ended %s tid %" PRIu64
               " archtid %u 0x%p listsz %d hashsz %d\n",
               info->name, info->tid, info->archtid, p,
               listc_size(&info->resource_list),
               hash_get_num_entries(info->resource_hash));

    thread_util_donework_int(p);

    hash_free(info->resource_hash);

    free(info->name);
    free(info);
}

void thread_util_register_describe_function(int type, void (*func)(void *))
{
    if (type < 0 || type >= MAX_RESOURCE_TYPE)
        return;
    describe_func[type] = func;
}

void thread_util_init(void)
{
    int rc;

    rc = pthread_key_create(&thread_util_key, thread_ended);
    if (rc) {
        printf("%s:%d pthread_key_create rc %d\n", __FILE__, __LINE__, rc);
        exit(1);
    }
    thread_started("main");
}

void thread_util_enable_debug(void)
{
    printf("Thread start/end debug enabled.\n");
    thread_debug = 1;
}

void thread_util_disable_debug(void)
{
    printf("Thread start/end debug disabled.\n");
    thread_debug = 0;
}

void thread_util_dump_on_exit_enable(void)
{
    printf("Dumping thread resources on exit enabled\n");
    dump_resources_on_thread_exit = 1;
}

void thread_util_dump_on_exit_disable(void)
{
    printf("Dumping thread resources on exit disabled\n");
    dump_resources_on_thread_exit = 0;
}

static void thread_util_donework_int(struct thread_info *info)
{
    struct thread_resource *r;
    int i;

    if (dump_resources_on_thread_exit) {
        LISTC_FOR_EACH(&info->resource_list, r, lnk)
        {
            if (r->type < 0 || r->type >= MAX_RESOURCE_TYPE) {
                printf("thread %" PRIu64
                       " archtid %u resource 0x%p unknown type %d\n",
                       info->tid, info->archtid, r->resource, r->type);
                continue;
            }
            if (describe_func[r->type]) {
                printf("thread %" PRIu64 " archtid %u:\n", info->tid,
                       info->archtid);
                describe_func[r->type](r->resource);
            } else {
                printf("thread %" PRIu64
                       " archtid %u still holds a type %d resource "
                       "0x%p at exit\n",
                       info->tid, info->archtid, r->type, r->resource);
            }
            for (i = 3; i < r->nframes; i++) {
                printf("0x%p ", r->stack[i]);
            }
            printf("\n");
        }
    }
    /* note: we leak these resources since we don't know how to deallocate them
       (maybe the add_resource
       should specify the free function?) */
    r = listc_rtl(&info->resource_list);
    while (r) {
        hash_del(info->resource_hash, r);
        free(r);
        r = listc_rtl(&info->resource_list);
    }
}

void thread_util_donework(void)
{
    struct thread_info *info;

    info = pthread_getspecific(thread_util_key);
    if (info == NULL) {
        printf("thread_util_donework called in thread that never called "
               "thread_started\n");
        return;
    }
    thread_util_donework_int(info);
}

#if defined(__linux__)

#include <unistd.h>
#include <sys/syscall.h>

arch_tid getarchtid(void) { return syscall(__NR_gettid); }

#elif defined(_IBM_SOURCE)

arch_tid getarchtid(void) { return thread_self(); }

#else

arch_tid getarchtid(void) { return pthread_self(); }

#endif
