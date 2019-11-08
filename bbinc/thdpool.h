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
 * Generic thread pool for comdb2.  This implementation will grow the pool
 * as much as it needs to in order to meet demand.
 */

#ifndef INC__THDPOOL_H
#define INC__THDPOOL_H

// comdb2ar is c++
#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <inttypes.h>
#include <priority_queue.h>

struct thdpool;

enum thdpool_ioctl_op { THD_RUN, THD_FREE };

/* Set some sane defaults for stacksize */
enum {
#if defined(_LINUX_SOURCE)

    DEFAULT_THD_STACKSZ = 1048576

#elif defined(_SUN_SOURCE)

    DEFAULT_THD_STACKSZ = 1048576

#elif defined(_IBM_SOURCE)

    DEFAULT_THD_STACKSZ = 163840

#elif defined(_HP_SOURCE)

    DEFAULT_THD_STACKSZ = 524288

#endif
};

typedef void (*thdpool_work_fn)(struct thdpool *pool, void *work, void *thddata,
                                int op);
struct workitem {
    void *work;
    thdpool_work_fn work_fn;
    int queue_time_ms;
    LINKC_T(struct workitem) linkv;
    int available;
    char *persistent_info;
    priority_t priority;
};

typedef void (*thdpool_thdinit_fn)(struct thdpool *pool, void *thddata);
typedef void (*thdpool_thddelt_fn)(struct thdpool *pool, void *thddata);
typedef void (*thdpool_thddque_fn)(struct thdpool *pool, struct workitem *item,
                                   int timeout);

typedef void (*thdpool_foreach_fn)(struct thdpool *pool, struct workitem *item,
                                   void *user);
void thdpool_foreach(struct thdpool *pool, thdpool_foreach_fn, void *user);

struct thdpool *thdpool_create(const char *name, size_t per_thread_data_sz);
void thdpool_destroy(struct thdpool **pool_p);
void thdpool_set_stack_size(struct thdpool *pool, size_t sz_bytes);
void thdpool_set_init_fn(struct thdpool *pool, thdpool_thdinit_fn init_fn);
void thdpool_set_delt_fn(struct thdpool *pool, thdpool_thddelt_fn delt_fn);
void thdpool_set_dque_fn(struct thdpool *pool, thdpool_thddque_fn dque_fn);
void thdpool_set_linger(struct thdpool *pool, unsigned lingersecs);
void thdpool_set_minthds(struct thdpool *pool, unsigned minnthd);
void thdpool_set_maxthds(struct thdpool *pool, unsigned minnthd);
void thdpool_set_maxqueue(struct thdpool *pool, unsigned maxqueue);
void thdpool_set_longwaitms(struct thdpool *pool, unsigned longwaitms);
void thdpool_set_maxqueueagems(struct thdpool *pool, unsigned maxqueueagems);
void thdpool_set_maxqueueoverride(struct thdpool *pool,
                                  unsigned maxqueueoverride);
void thdpool_set_mem_size(struct thdpool *pool, size_t sz_bytes);

int thdpool_get_queue_depth(struct thdpool *pool);

void thdpool_print_stats(FILE *fh, struct thdpool *pool);

enum {
    THDPOOL_ENQUEUE_FRONT = 0x1,
    THDPOOL_FORCE_DISPATCH = 0x2,
    THDPOOL_FORCE_QUEUE = 0x4,
    THDPOOL_QUEUE_ONLY = 0x8
};
int thdpool_enqueue(struct thdpool *pool, thdpool_work_fn work_fn, void *work,
                    int queue_override, char *persistent_info, uint32_t flags,
                    priority_t priority);
void thdpool_stop(struct thdpool *pool);
void thdpool_resume(struct thdpool *pool);
void thdpool_set_exit(struct thdpool *pool);
void thdpool_set_wait(struct thdpool *pool, int wait);
void thdpool_process_message(struct thdpool *pool, char *line, int lline,
                             int st);
char *thdpool_get_name(struct thdpool *pool);
priority_t thdpool_get_highest_priority(struct thdpool *pool);
int thdpool_get_status(struct thdpool *pool);
int thdpool_get_nthds(struct thdpool *pool);
int thdpool_get_nfreethds(struct thdpool *pool);
void thdpool_add_waitthd(struct thdpool *pool);
void thdpool_remove_waitthd(struct thdpool *pool);
int thdpool_get_maxthds(struct thdpool *pool);
int thdpool_get_peaknthds(struct thdpool *pool);
int thdpool_get_creates(struct thdpool *pool);
int thdpool_get_exits(struct thdpool *pool);
int thdpool_get_passed(struct thdpool *pool);
int thdpool_get_enqueued(struct thdpool *pool);
int thdpool_get_dequeued(struct thdpool *pool);
int thdpool_get_timeouts(struct thdpool *pool);
int thdpool_get_failed_dispatches(struct thdpool *pool);
int thdpool_get_minnthd(struct thdpool *pool);
int thdpool_get_maxnthd(struct thdpool *pool);
int thdpool_get_peakqueue(struct thdpool *pool);
int thdpool_get_maxqueue(struct thdpool *pool);
int thdpool_get_nqueuedworks(struct thdpool *pool);
int thdpool_get_longwaitms(struct thdpool *pool);
int thdpool_get_lingersecs(struct thdpool *pool);
int thdpool_get_stacksz(struct thdpool *pool);
int thdpool_get_maxqueueoverride(struct thdpool *pool);
int thdpool_get_maxqueueagems(struct thdpool *pool);
int thdpool_get_exit_on_create_fail(struct thdpool *pool);
int thdpool_get_dump_on_full(struct thdpool *pool);
void thdpool_list_pools(void);
void thdpool_command_to_all(char *line, int lline, int st);
void thdpool_set_dump_on_full(struct thdpool *pool, int onoff);
/* TODO: maybe thdpool_set_event_callback, to call for various life cycle events? */
void thdpool_set_queued_callback(struct thdpool *pool, void(*callback)(void*));

int thdpool_lock(struct thdpool *pool);
int thdpool_unlock(struct thdpool *pool);

struct thdpool *thdpool_next_pool(struct thdpool *pool);


#ifdef __cplusplus
}
#endif

#endif /* INC__THDPOOL_H */
