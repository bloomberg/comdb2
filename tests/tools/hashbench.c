#include <stdio.h>
#include <inttypes.h>
#include <nodemap.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <intern_strings.h>
#include <time.h>
#include <plhash.h>
#include "mem.h"
#include <pthread.h>
#include "testutil.h"

static char *argv0=NULL;

enum
{
    MODE_NODEIX     = 1,
    MODE_STRING     = 2,
    MODE_ALLOC      = 3,
    MODE_NODEIXIX   = 4
};

void usage(FILE *f)
{
    fprintf(stderr, "Usage: %s <cmd-line>\n", argv0);
    fprintf(stderr, " -i <iterations>       - iterations\n");
    fprintf(stderr, " -c <host-count>       - hostname count\n");
    fprintf(stderr, " -m <host-min>         - hostname min-string\n");
    fprintf(stderr, " -x <host-max>         - hostname max-string\n");
    fprintf(stderr, " -n                    - nodeix-hash mode\n");
    fprintf(stderr, " -s                    - string-hash mode\n");
    fprintf(stderr, " -a                    - string-hash-alloc mode\n");
    fprintf(stderr, " -e                    - nodeix-index mode\n");
    fprintf(stderr, " -h                    - help\n");
}

static char randc(void)
{
    const char *characters="abcdefghijklmnopqrstuvwxyz0123456789";
    static int len = 0;
    if (len == 0) len = strlen(characters);
    return characters[random() % len];
}

static char *randstring(int minsz, int maxsz)
{
    int sz = (minsz == maxsz) ? minsz: minsz + (random() % (maxsz - minsz));
    char *r = (char *)malloc(sz + 1);
    for (int i = 0; i < sz; i++) {
        r[i] = randc();
    }
    r[sz] = '\0';
    return r;
}

static char **create_random_strings(int count, int minsz, int maxsz)
{
    char **r = (char **)malloc(count * sizeof(char *));
    for (int i = 0; i < count; i++) {
        r[i] = intern(randstring(minsz, maxsz));
    }
    return r;
}

struct int_nodeix {
    int nodeix;
    int i;
};

struct int_strptr {
    const char *host;
    int i;
};

hash_t *nodex_hash;
hash_t *str_hash;
int *nodeix_index;

int *retrieve_strp_alloc(const char *inhost)
{
    struct int_strptr *n;
    const char *host = intern(inhost);
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lk);
    if ((n = hash_find(str_hash, &host)) == NULL) {
        n = calloc(sizeof(struct int_strptr), 1);
        n->host = strdup(host);
        hash_add(str_hash, n);
    }
    pthread_mutex_unlock(&lk);
    return &n->i;
}

int *retrieve_strp(const char *inhost)
{
    struct int_strptr *n;
    const char *host = intern(inhost);
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lk);
    if ((n = hash_find(str_hash, &host)) == NULL) {
        n = calloc(sizeof(struct int_strptr), 1);
        n->host = host;
        hash_add(str_hash, n);
    }
    pthread_mutex_unlock(&lk);
    return &n->i;
}

int *retrieve_nodex(const char *host)
{
    int nix = nodeix(host);
    struct int_nodeix *n;
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_lock(&lk);
    if ((n = hash_find(nodex_hash, &nix)) == NULL) {
        n = calloc(sizeof(struct int_nodeix), 1);
        n->nodeix = nix;
        hash_add(nodex_hash, n);
    }
    pthread_mutex_unlock(&lk);
    return &n->i;
}

int *retrieve_nodex_index(const char *host)
{
    int nix = nodeix(host);
    return &nodeix_index[nix];
}

char *mode_to_str(int mode)
{
    switch(mode) {
        case MODE_STRING:
            return "string-hash";
            break;
        case MODE_ALLOC:
            return "string-alloc-hash";
            break;
        case MODE_NODEIX:
            return "nodex-hash";
            break;
        case MODE_NODEIXIX:
            return "nodex-index";
            break;
        default:
            return "unknown";
    }
}

/* Simple benchmark for different hash-ing algorithms */
int main(int argc, char *argv[])
{
    int c, hostcount=64, minsz=16, maxsz=32, iterations=1000000, mode=MODE_NODEIX, *n;
    uint64_t startms, endms;
    argv0=argv[0];
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);
    srandom(time(NULL) ^ getpid());
    comdb2ma_init(0, 0);

    while ((c = getopt(argc, argv, "hc:m:x:i:nsaeh"))!=EOF) {
        switch(c) {
            case 'i':
                iterations = atoi(optarg);
                fprintf(stderr, "Set iterations to %d\n", iterations);
                break;

            case 'c':
                hostcount = atoi(optarg);
                fprintf(stderr, "Set hostcount to %d\n", hostcount);
                break;

            case 'm':
                minsz = atoi(optarg);
                fprintf(stderr, "Set minsz to %d\n", minsz);
                break;

            case 'x':
                maxsz = atoi(optarg);
                fprintf(stderr, "Set maxsz to %d\n", maxsz);
                break;

            case 'n':
                mode = MODE_NODEIX;
                fprintf(stderr, "Set mode to NODEIX-HASH\n");
                break;

            case 's':
                mode = MODE_STRING;
                fprintf(stderr, "Set mode to STRING-HASH\n");
                break;

            case 'a':
                mode = MODE_ALLOC;
                fprintf(stderr, "Set mode to STRING-ALLOC-HASH\n");
                break;

            case 'e':
                mode = MODE_NODEIXIX;
                fprintf(stderr, "Set mode to NODEIX-INDEX\n");
                break;


            case 'h':
                usage(stdout);
                exit(0);
                break;

            default:
                usage(stderr);
                exit(1);
                break;
        }
    }

    if (minsz > maxsz || minsz <= 0) {
        fprintf(stderr, "Invalid minsz\n");
        usage(stderr);
        exit(1);
    }

    char **r = create_random_strings(hostcount, minsz, maxsz);

    if (mode == MODE_STRING || mode == MODE_ALLOC) {
        str_hash = hash_init_strptr(offsetof(struct int_strptr, host));
    } else if (mode == MODE_NODEIX) {
        nodex_hash = hash_init(sizeof(int));
    } else if (mode == MODE_NODEIXIX) {
        nodeix_index = calloc(sizeof(int), hostcount);
    }

    startms = timems();

    for (int i = 0; i < iterations; i++) {
        if (mode == MODE_STRING) {
            n = retrieve_strp(r[random() % hostcount]);
            (*n) = random();
        } else if (mode == MODE_ALLOC){
            n = retrieve_strp_alloc(r[random() % hostcount]);
            (*n) = random();
        } else if (mode == MODE_NODEIX){
            n = retrieve_nodex(r[random() % hostcount]);
            (*n) = random();
        } else if (mode == MODE_NODEIXIX) {
            n = retrieve_nodex_index(r[random() % hostcount]);
            (*n) = random();
        }
    }
    endms = timems();
    fprintf(stderr, "%s test %d iterations in %"PRIu64"ms %f/s\n", mode_to_str(mode),
        iterations, endms - startms, (double)(((double)iterations * (double)1000) / (double)(endms - startms)));
    return 0;
}
