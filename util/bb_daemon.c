#include <bb_daemon.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

int bb_daemon(void)
{
    pid_t pid;

    if (chdir("/") != 0) return -1;

    int fd;
    if ((fd = open("/dev/null", O_RDWR)) < 0) return -1;

    if (dup2(fd, 0) < 0 || dup2(fd, 1) < 0 || dup2(fd, 2) < 0) {
        close(fd);
        return -1;
    }

    close(fd);
    if ((pid = fork()) < 0) return -1;

    if (pid > 0) _exit(0);

    if (setsid() < 0) return -1;

    return 0;
}
