#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <pthread.h>

#include "thread_util.h"
#include "thdpool.h"
#include "comdb2_atomic.h"
#include "mem.h"

int gbl_disable_exit_on_thread_error;
int gbl_throttle_sql_overload_dump_sec;

void register_tunable(void *tunable) 
{
}
void thdpool_alarm_on_queing(int len)
{
}

typedef struct { 
    int spawned_count;
    uint32_t completed_count;
    uint32_t sum;
} common_t;

typedef struct { 
    int id;
    common_t *c;
} info_t;

static void handler_work_pp(struct thdpool *pool, void *work, void *thddata, int op)
{
    info_t *info = work; 
    ATOMIC_ADD32(info->c->sum, info->id);
    usleep(rand() % 1000);
    ATOMIC_ADD32(info->c->completed_count, 1);
    free(work);
}

int main()
{
    comdb2ma_init(0, 0);
    thread_util_init();
    struct thdpool *my_thdpool = thdpool_create("my_pool", 0);

    assert(my_thdpool);

    //thdpool_set_init_fn(my_thdpool, my_thd_start);
    thdpool_set_minthds(my_thdpool, 0);
    thdpool_set_maxthds(my_thdpool, 8);
    thdpool_set_linger(my_thdpool, 0);
    thdpool_set_longwaitms(my_thdpool, 1000000);
    thdpool_set_maxqueue(my_thdpool, 100);
    thdpool_set_mem_size(my_thdpool, 4 * 1024);
    common_t c = {0};
    const int MAX = 100;

    for (int i = 1; i <= MAX; i++) {
        info_t *work = calloc(1, sizeof(info_t));
        work->id = i;
        work->c = &c;
        c.spawned_count++;
        int rc = thdpool_enqueue(my_thdpool, handler_work_pp, work, 0, NULL, 
                THDPOOL_FORCE_QUEUE, PRIORITY_T_DEFAULT);
        if (rc) {
            fprintf(stderr, "Error from thdpool_enqueue, rc=%d\n", rc);
            exit(1);
        }
    }

    //while (thdpool_get_nthds() c.completed_count < c.spawned_count) 
    while (thdpool_get_nthds(my_thdpool) + thdpool_get_nfreethds(my_thdpool) > 0) {
        printf("Waiting for thdpool %d/%d done\n", c.completed_count, c.spawned_count);
        sleep(1);
    }

    printf("Work completed thdpool %d/%d done\n", c.completed_count, c.spawned_count);
    if (c.sum != (MAX+1)*MAX/2)
        abort();

    printf("Done waiting for thdpool, now cleanup\n");
    thdpool_stop(my_thdpool);
    sleep(1);
    thdpool_destroy(&my_thdpool);
    // this will remove the mspace and we won't be able to see any leaks,
    // so keep this commented out:
    //comdb2ma_exit();
    pthread_exit(NULL); // call any key destructors
}
