#include <dlfcn.h>
#include <unistd.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>

int __partial_write_stall = 0;
ssize_t (*libcwrite)(int fd, const void *buf, size_t count) = NULL;
ssize_t write(int fd, const void *buf, size_t count)
{
    ssize_t nw;
    struct stat s;
    int nsleep;
    if (libcwrite == NULL) {
        libcwrite = dlsym(RTLD_NEXT, "write");
        if (libcwrite == NULL) {
            fprintf(stderr, "error finding system write\n");
            abort();
        }
    }
    if (fd == -1)
        return -1;

    if (fstat(fd, &s) != 0) {
        perror("fstat");
        return -1;
    }

    if (!S_ISSOCK(s.st_mode) || __partial_write_stall == 0)
        return libcwrite(fd, buf, count);

    srand(time(NULL));
    int nn = (rand() % count) + 1;
    fprintf(stderr, "Performing a partial write of %d out %zu bytes and then stalling\n", nn, count);
    nw = libcwrite(fd, buf, nn);
    nsleep = __partial_write_stall;
    __partial_write_stall = 0;
    sleep(nsleep);
    return nw;
}
