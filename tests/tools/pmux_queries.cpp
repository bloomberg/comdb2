#include <string>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <time.h>
#include <sstream>
#include <iostream>
#include <getopt.h>

typedef struct {
    unsigned int thrid;
    unsigned int pmuxport;
    unsigned int count;
} thr_info_t;

int readtimeoutms = 20;
int connecttimeoutms = 10;
int speedtest = 0;

int timed_read(int sfd, char *buf, int len)
{
    int rc;
    struct pollfd pol;

    if (readtimeoutms > 0) {
        do {
            pol.fd = sfd;
            pol.events = POLLIN;
            rc = poll(&pol, 1, readtimeoutms);
        } while (rc == -1 && errno == EINTR);

        if (rc == 0) {
            fprintf(stderr, "Read timeout (poll rc=%d)\n", rc);
            return -1;
        } 
        if (rc < 0) {
            perror("Error from poll:");
            return -1;
        }
        if ((pol.revents & POLLIN) == 0)
            return -100000 + pol.revents;
        /*something to read*/
    }
    return read(sfd, buf, len);
}

int send_and_read_int(int sfd, const std::string & str)
{
    int len = str.length();
    if (write(sfd, str.c_str(), len) != len) {
        fprintf(stderr, "client partial/failed write\n");
        exit(EXIT_FAILURE);
    }

    char buf[128];
    int nread = timed_read(sfd, buf, sizeof(buf));
    if (nread < 0) {
        exit(EXIT_FAILURE);
    }
    int resp = atoi(buf);
    return resp;
}

void timed_connect(int *sfd, int pmuxport)
{
    *sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (*sfd == -1) {
        perror("socket:");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in bindaddr = {0};
    bindaddr.sin_family = AF_INET;
    bindaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    bindaddr.sin_port = htons(pmuxport);

    int saved_flags = fcntl(*sfd, F_GETFL, 0);

    if (connecttimeoutms <= 0 || saved_flags < 0) {
        if (saved_flags < 0)
            perror("Can't get socket info:");

        int rc = connect(*sfd, (struct sockaddr *)&bindaddr, sizeof(bindaddr));
        if (rc == -1) {
            perror("connect: ");
            exit(EXIT_FAILURE);
        }
        return;
    }

    if(fcntl(*sfd, F_SETFL, saved_flags | O_NONBLOCK) < 0) {
        perror("set NONBLOCK: ");
        exit(EXIT_FAILURE);
    }

    int rc = connect(*sfd, (struct sockaddr *)&bindaddr, sizeof(bindaddr));
    if (rc == -1) {
        if(errno != EINPROGRESS) {
            perror("failed connect: ");
            exit(EXIT_FAILURE);
        }
        struct pollfd pol;
        do {
            pol.fd = *sfd;
            pol.events = POLLOUT;
            rc = poll(&pol, 1, connecttimeoutms);
        } while (rc == -1 && errno == EINTR);
        if (rc == -1) {
            perror("poll error\n");
            exit(EXIT_FAILURE);
        }
        if (rc == 0) {
            fprintf(stderr, "connect timeout\n");
            exit(EXIT_FAILURE);
        }
        if ((pol.revents & POLLOUT) == 0) {
            fprintf(stderr, "not ready after timeout\n");
            exit(EXIT_FAILURE);
        }
    }
    if (fcntl(*sfd, F_SETFL, saved_flags) < 0) {
        fprintf(stderr, "can't set back flags\n");
        exit(EXIT_FAILURE);
    }
}

void build_msg(const char *pre, std::string &name)
{
}

void *thr(void *arg)
{
    int getport = 0;
    int regport = 0;
    thr_info_t *t = (thr_info_t *) arg;
    srand(time(NULL));
    std::ostringstream ss;
    ss << "comdb2/replication/pmuxtest" << t->thrid << random() % 10000;
    std::string name = ss.str();
    int sfd;

    timed_connect(&sfd, t->pmuxport);
    int minport = send_and_read_int(sfd, "range\n");
    if (minport <= 1) {
        std::cerr << "Minport is not correct: " << minport << std::endl;
        exit(EXIT_FAILURE);
    }

    for(int ii = 0; ii < t->count; ii++) {
        if (!speedtest || ii == 0) { // if we are doing speedtest, only reg once
            regport = send_and_read_int(sfd, "reg " + name + "\n");
            if (regport < minport) {
                std::cerr << "reg port is not correct " << regport << std::endl;
                exit(EXIT_FAILURE);
            }
        }

        getport = send_and_read_int(sfd, "get " + name + "\n");
        if (regport != getport) {
            std::cerr << "get port " << getport << " is not the same as regport " << regport << std::endl;
            exit(EXIT_FAILURE);
        }

        if (!speedtest || ii == t->count - 1) { // if we are doing speedtest, only del once
            getport = send_and_read_int(sfd, "del " + name + "\n");
            if (0 != getport) {
                std::cerr << "del port " << getport << " is not 0" << std::endl;
                exit(EXIT_FAILURE);
            }

            getport = send_and_read_int(sfd, "get " + name + "\n");
            if (-1 != getport) {
                std::cerr << "get port " << getport << " is not -1 " << std::endl;
                exit(EXIT_FAILURE);
            }
        }
    }
    close(sfd);

    std::cout << "Done thr " << t->thrid << std::endl;
    return NULL;
}

void usage(const char *p, const char *err) {
    fprintf(stderr, "%s\n", err);
    fprintf(stderr, "Usage %s --pmuxport PORT --numthreads NUMTHREADS --iterations ITERATIONS \n", p);
    exit(1);
}

int main(int argc, char *argv[])
{
    int pmuxport = 0;
    int numthreads = 0;
    int iterations = 0;

    if(argc < 3)
        usage(argv[0], "Required parameters were NOT provided\n"); //exit too

    static struct option long_options[] =
    {
        //{char *name; int has_arg; int *flag; int val;}
        {"pmuxport", required_argument, NULL, 'p'},
        {"numthreads", required_argument, NULL, 'n'},
        {"iterations", required_argument, NULL, 'i'},
        {"speedtest", optional_argument, NULL, 's'},
        {NULL, 0, NULL, 0}
    };

    int c;
    int index;
    while ((c = getopt_long(argc, argv, "p:n:i:s?", long_options, &index))!=EOF) {
        //printf("c '%c' %d index %d optarg '%s'\n", c, c, index, optarg);
        switch(c) {
            case 'n': numthreads = atoi(optarg); break;
            case 'p': pmuxport = atoi(optarg); break;
            case 'i': iterations = atoi(optarg); break;
            case 's': speedtest = true; break;
            case '?':  break;
            default: break;
        }
    }

    if (pmuxport < 1)
        usage(argv[0], "Parameter pmuxport is not set\n"); //exit too
    if (numthreads < 1)
        usage(argv[0], "Parameter numthreads is not set\n"); //exit too
    if (iterations < 1)
        usage(argv[0], "Parameter iterations is not set\n"); //exit too

    printf("%d %d %d\n", pmuxport, numthreads, iterations);

    pthread_t *t = (pthread_t *) malloc(sizeof(pthread_t) * numthreads);
    thr_info_t *tinfo = (thr_info_t *) malloc(sizeof(thr_info_t) * numthreads);
    fprintf(stderr, "starting %d threads\n", numthreads);

    /* create threads */
    for (unsigned long long i = 0; i < numthreads; ++i) {
        tinfo[i].thrid = i;
        tinfo[i].pmuxport = pmuxport;
        tinfo[i].count = iterations;
        pthread_create(&t[i], NULL, thr, (void *)&tinfo[i]);
    }

    void *r;
    for (unsigned int i = 0; i < numthreads; ++i)
        pthread_join(t[i], &r);

    std::cout << "Done Main" << std::endl;
    return 0;
}
