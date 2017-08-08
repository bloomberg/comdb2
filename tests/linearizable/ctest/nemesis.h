#ifndef _NEMESIS_H
#define _NEMESIS_H

#include <inttypes.h>

struct nemesis {
    char *dbname;
    char *cltype;
    char *master;
    int numnodes;
    char **cluster;
    int *ports;
    uint32_t flags;
};

/* Flags */
#define NEMESIS_PARTITION_MASTER 0x0001
#define NEMESIS_VERBOSE 0x0002

struct nemesis *nemesis_open(char *dbname, char *cltype, uint32_t flags);
void nemesis_close(struct nemesis *n);
void breaknet(struct nemesis *n);
void fixnet(struct nemesis *n);
void signaldb(struct nemesis *n, int signal, int signal_all);
void breakclocks(struct nemesis *n, int maxskew);
void fixclocks(struct nemesis *n);
void fixall(struct nemesis *n);

#endif
