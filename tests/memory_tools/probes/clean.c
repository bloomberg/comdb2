/* Negative control: correct allocation and threading. Every memory-tool
 * variant runs this and requires exit status 0 and no detector report. */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static long total;

static void *worker(void *arg)
{
    long n = (long)arg;
    char *buf = malloc(128);
    if (buf == NULL)
        return NULL;
    memset(buf, 'c', 128);
    pthread_mutex_lock(&lock);
    total += n + buf[n];
    pthread_mutex_unlock(&lock);
    free(buf);
    return NULL;
}

int main(void)
{
    pthread_t threads[2];
    long i;

    printf("memory-probe: clean\n");
    fflush(stdout);
    for (i = 0; i < 2; i++) {
        if (pthread_create(&threads[i], NULL, worker, (void *)i) != 0)
            return 2;
    }
    for (i = 0; i < 2; i++)
        pthread_join(threads[i], NULL);
    return total == 0 ? 3 : 0;
}
