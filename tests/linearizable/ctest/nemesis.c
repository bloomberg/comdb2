#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <alloca.h>
#include <cdb2api.h>
#include <signal.h>
#include <testutil.h>
#include <nemesis.h>

#define MAXCL 64

#define ANSI_COLOR_WHITE   "\x1b[37;1m"
#define ANSI_COLOR_RESET   "\x1b[0m"

static int setcluster(struct nemesis *n, char *dbname, char *cltype,
                      uint32_t flags)
{
    int rc, count = 0;
    cdb2_hndl_tp *db;

    while ((rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM)) != 0) {
        fprintf(stderr, "%s cdb2_open returns %d\n", __func__, rc);
        cdb2_close(db);
        if (++count > 100) {
            fprintf(stderr, "%s could not database: %d\n", __func__, rc);
            return -1;
        }
        sleep(1);
    }

    n->dbname = strdup(dbname);
    n->cltype = strdup(cltype);
    n->cluster = (char **)malloc(sizeof(char *) * MAXCL);
    n->ports = (int *)malloc(sizeof(int) * MAXCL);
    n->flags = flags;

    rc = cdb2_run_statement(db, "set hasql on");
    do {
        rc = cdb2_next_record(db);
    } while (rc == CDB2_OK);

    cdb2_cluster_info(db, n->cluster, n->ports, MAXCL, &n->numnodes);
    cdb2_close(db);

    n->master = master(n->dbname, n->cltype);

    return 0;
}

void fixnet(struct nemesis *n)
{
    char cmd[2048];
    char c[256];

    for (int count = 0; count < 2; count++) {
        for (int i = 0; i < n->numnodes; i++) {

            cmd[0] = 0;
            sprintf(c, "ssh %s \"", n->cluster[i]);
            strcat(cmd, c);

            for (int j = 0; j < n->numnodes; j++) {
//# sudo iptables -F -w; sudo iptables -X -w
                if (n->flags & NEMESIS_PARTITION_WHOLE_NETWORK) {
                    sprintf(c, "sudo iptables -F -w; "
                            "sudo iptables -X -w; ");
                    strcat(cmd, c);
                }
                else {
                    sprintf(c, "sudo iptables -D INPUT -s %s -p tcp "
                            "--destination-port %d -j DROP -w;",
                            n->cluster[j], n->ports[j]);
                    strcat(cmd, c);
                    sprintf(c, "sudo iptables -D INPUT -s %s -p udp "
                            "--destination-port %d -j DROP -w ; ",
                            n->cluster[j], n->ports[j]);
                    strcat(cmd, c);
                }
            }
            strcat(cmd, "\" < /dev/null >/dev/null 2>&1");
            if (n->flags & NEMESIS_VERBOSE) printf("%s\n", cmd);
            system(cmd);
        }
    }
    printf("Fully connected\n");
}

void breaknet(struct nemesis *n)
{
    char cmd[1024];
    char c[256];
    int *node = alloca(n->numnodes * sizeof(int));
    int n1 = -1, n2, i;

    n->master = master(n->dbname, n->cltype);
    bzero(node, n->numnodes * sizeof(int));

    if (n->master && (n->flags & NEMESIS_PARTITION_MASTER)) {
        for (i = 0; i < n->numnodes; i++) {
            if (n->master && !strcmp(n->cluster[i], n->master)) {
                n1 = i;
                break;
            }
        }

        if (n1 == -1) {
            fprintf(stderr, "Error: couldn't find master (?)\n");
            abort();
        }
    } else
        n1 = rand() % n->numnodes;

    node[n1] = 1;
    do {
        n2 = rand() % n->numnodes;
    } while (n2 == n1);
    node[n2] = 1;

    if (n->flags & NEMESIS_COLOR_PRINT) {
        printf(ANSI_COLOR_WHITE);
    }
    printf("Cut off %s %s from cluster, master is %s", 
            n->cluster[n1], n->cluster[n2], n->master);

    if (n->flags & NEMESIS_COLOR_PRINT) {
        printf(ANSI_COLOR_RESET);
    }
    printf("\n");
    for (int broken = 0; broken < n->numnodes; broken++) {
        if (node[broken]) {
            cmd[0] = 0;
            sprintf(c, "ssh %s \"", n->cluster[broken]);
            strcat(cmd, c);
            for (int working = 0; working < n->numnodes; working++) {
                if (!node[working]) {
                    if (n->flags & NEMESIS_PARTITION_WHOLE_NETWORK) {
                        sprintf(c, "sudo iptables -A INPUT -s %s -p tcp "
                                "-j DROP -w;",
                                n->cluster[working]);
                    }
                    else {
                        sprintf(c, "sudo iptables -A INPUT -s %s -p tcp "
                                "--destination-port %d -j DROP -w;",
                                n->cluster[working], n->ports[working]);
                    }
                    strcat(cmd, c);

                    if (n->flags & NEMESIS_PARTITION_WHOLE_NETWORK) {
                        sprintf(c, "sudo iptables -A INPUT -s %s -p udp "
                                "-j DROP -w;",
                                n->cluster[working]);
                    }
                    else {
                        sprintf(c, "sudo iptables -A INPUT -s %s -p udp "
                                "--destination-port %d -j DROP -w;",
                                n->cluster[working], n->ports[working]);
                    }
                    strcat(cmd, c);
                }
            }
            strcat(cmd, "\" < /dev/null >/dev/null 2>&1");
            if (n->flags & NEMESIS_VERBOSE) printf("%s\n", cmd);
            system(cmd);
        }
    }
}

void signaldb(struct nemesis *n, int signal, int all)
{
    char cmd[1024];
    int *node = alloca(n->numnodes * sizeof(int));

    bzero(node, n->numnodes * sizeof(int));
    int n1 = -1, n2, i;

    if (!all) {
        if (n->master && (n->flags & NEMESIS_PARTITION_MASTER)) {
            for (i = 0; i < n->numnodes; i++) {
                if (n->master && !strcmp(n->cluster[i], n->master)) {
                    n1 = i;
                    break;
                }
            }

            if (n1 == -1) {
                fprintf(stderr, "Error: couldn't find master (?)\n");
                abort();
            }
        } else
            n1 = rand() % n->numnodes;

        node[n1] = 1;
        do {
            n2 = rand() % n->numnodes;
        } while (n2 == n1);
        node[n2] = 1;
    }

    printf("signaling %d on ", signal);
    for (int x = 0; x < n->numnodes; x++) {
        if (node[x] || all) {
            printf("%s ", n->cluster[x]);
        }
    }
    printf("\n");

    for (int broken = 0; broken < n->numnodes; broken++) {
        if (node[broken] || all) {
            sprintf(cmd, "ssh %s \"sudo kill -%d \\$(cat /tmp/%s.pid)\""
                         " < /dev/null > /dev/null 2>&1\n",
                    n->cluster[broken], signal, n->dbname);
            if (n->flags & NEMESIS_VERBOSE) printf("%s", cmd);
            fflush(stdout);
            system(cmd);
        }
    }
}

void breakclocks(struct nemesis *n, int maxskew)
{
    char cmd[1024];
    printf("Breaking clocks\n");
    for (int x = 0; x < n->numnodes; x++) {
        sprintf(cmd,
                "ssh %s \"cur=\\$(date +%%s) ; maxskew=%d ; "
                "newtime=\\$(( cur + (maxskew / 2) - (RANDOM %% maxskew) )) ; "
                "sudo date \\\"+%%s\\\" -s \\@\\$newtime\" < /dev/null >/dev/null 2>&1\n",
                n->cluster[x], maxskew);
        if (n->flags & NEMESIS_VERBOSE) printf("%s", cmd);
        system(cmd);
    }
}

void fixclocks(struct nemesis *n)
{
    char cmd[1024];
    printf("Fixing clocks\n");
    for (int x = 0; x < n->numnodes; x++) {
        sprintf(cmd, "ssh %s \"sudo service ntp stop ; sudo service ntp "
                     "start\" < /dev/null >/dev/null 2>&1\n",
                n->cluster[x]);
        if (n->flags & NEMESIS_VERBOSE) printf("%s", cmd);
        fflush(stdout);
        system(cmd);
    }
}

struct nemesis *nemesis_open(char *dbname, char *cltype, uint32_t flags)
{
    struct nemesis *n = calloc(sizeof(*n), 1);
    if (setcluster(n, dbname, cltype, flags)) {
        nemesis_close(n);
        return (NULL);
    }
    return n;
}

void nemesis_close(struct nemesis *n)
{
    if (n == NULL) return;
    if (n->dbname) free(n->dbname);
    if (n->cltype) free(n->cltype);
    if (n->cluster) free(n->cluster);
    if (n->ports) free(n->ports);
    if (n->master) free(n->master);
    free(n);
}

void fixall(struct nemesis *n)
{
    signaldb(n, SIGCONT, 1);
    fixnet(n);
    fixclocks(n);
}
