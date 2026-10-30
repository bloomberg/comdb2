/* Deliberate data race: two threads update an ordinary shared variable with
 * no synchronization between them. Both threads are joined before exit.
 * Expected detector: ThreadSanitizer "data race". */

#include <pthread.h>
#include <stdio.h>

static int shared_counter;

static void *racer(void *arg)
{
    int i;
    (void)arg;
    for (i = 0; i < 10000; i++)
        shared_counter++;
    return NULL;
}

int main(void)
{
    pthread_t a, b;

    printf("memory-probe: data-race\n");
    fflush(stdout);
    if (pthread_create(&a, NULL, racer, NULL) != 0)
        return 2;
    if (pthread_create(&b, NULL, racer, NULL) != 0)
        return 2;
    pthread_join(a, NULL);
    pthread_join(b, NULL);
    return shared_counter == 0 ? 3 : 0;
}
