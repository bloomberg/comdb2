/* Dumb test to verify that the atomics function calls that we use
 * actually do work
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <comdb2_atomic.h>

#define FORL(ii,s,e) for(int ii = s; ii < e; ii++)
#define THREADNUM 10
#define NUMTOADD 1000000

void * add_a_million(void *arg)
{
    uint32_t *a = arg;
    FORL(i, 0, NUMTOADD) {
        ATOMIC_ADD32(*a, 1);
    }
    return NULL;
}


int main()
{
    //call 10 threads to increase variable passed in
    int a = 0;
    pthread_t thv[THREADNUM];
    FORL(i, 0, THREADNUM) {
        pthread_create(&thv[i], NULL, add_a_million, &a);
    }

    FORL(i, 0, THREADNUM) {
        void *val;
        pthread_join(thv[i], &val);
    }
    assert(a == THREADNUM * NUMTOADD);
    return 0;
}
