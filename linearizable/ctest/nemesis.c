#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <alloca.h>
#include <cdb2api.h>
#include <signal.h>
#include <nemesis.h>

#define MAXCL 64

static int setcluster(struct nemesis *n, char *dbname, char *cltype, uint32_t flags)
{
    int rc, count=0;
    cdb2_hndl_tp *db;
    while ((rc = cdb2_open(&db, dbname, cltype, CDB2_RANDOM))!=0) {
        fprintf(stderr, "%s cdb2_open returns %d\n", __func__, rc);
        cdb2_close(db);
        if (++count > 100) {
            fprintf(stderr, "%s could not database: %d\n", __func__, rc);
            return -1;
        }
        sleep (1);
    }

    n->dbname = strdup(dbname);
    n->cltype = strdup(cltype);
    n->cluster = (char **)malloc(sizeof(char *) * MAXCL);
    n->ports = (int *)malloc(sizeof(int) * MAXCL);
    n->flags = flags;

    cdb2_cluster_info(db, n->cluster, n->ports, MAXCL, &n->numnodes);

    rc = cdb2_run_statement(db, "set hasql on"); 
    do {
        rc = cdb2_next_record(db);
    } while (rc == CDB2_OK);

    rc = cdb2_run_statement(db, "exec procedure sys.cmd.send('bdb cluster')");
    if (rc) {
        fprintf(stderr, "exec procedure sys.cmd.send: %d %s\n", rc, 
                cdb2_errstr(db));
        cdb2_close(db);
        return -1;
    }

    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        char *f, *s, *m;
        s = cdb2_column_value(db, 0);
        if (strstr(s, "MASTER")) {
            while (*s && isspace(*s)) s++;
            char *endptr;
            f = m = strdup(s);
            m = strtok_r(m, ":", &endptr);
            if (m) {
                n->master = strdup(m);
            }
            free(f);
        }
        rc = cdb2_next_record(db);
    }
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "next master rc %d %s\n", rc, cdb2_errstr(db));
        cdb2_close(db);
        return -1;
    }

    return 0;
}

void fixnet(struct nemesis *n) 
{
    char cmd[2048];
    char c[256];

    for (int count = 0 ; count < 2 ; count++) {
        for (int i = 0; i < n->numnodes; i++) {

            cmd[0] = 0;
            sprintf(c,  "ssh %s \"", n->cluster[i]);
            strcat(cmd, c);

            for (int j = 0 ; j < n->numnodes; j++) {
                sprintf(c, "sudo iptables -D INPUT -s %s -p tcp --destination-port %d -j DROP -w;", n->cluster[j], n->ports[j]);
                strcat(cmd, c);
                sprintf(c, "sudo iptables -D INPUT -s %s -p udp --destination-port %d -j DROP -w;", n->cluster[j], n->ports[j]);
                strcat(cmd, c);
            }
            strcat(cmd, "\" < /dev/null");
            printf("%s\n", cmd);
            system(cmd);
        }
    }
}

void breaknet(struct nemesis *n) 
{
    char cmd[1024];
    char c[256];
    int *node = alloca(n->numnodes * sizeof(int));
    int n1 = -1, n2, i;

    bzero(node, n->numnodes * sizeof(int));

    if (n->master && (n->flags & PARTITION_MASTER))
    {
        for(i=0;i<n->numnodes;i++)
        {
            if (n->master && !strcmp(n->cluster[i], n->master))
            {
                n1 = i;
                break;
            }
        }

        if (n1 == -1)
        {
            fprintf(stderr, "Error: couldn't find master (?)\n");
            abort();
        }
    }
    else
        n1 = rand() % n->numnodes;

    node[n1] = 1;
    do {
        n2 = rand() % n->numnodes;
    } while (n2 == n1);
    node[n2] = 1;

    printf("cutting off %s %s from cluster\n", n->cluster[n1], n->cluster[n2]);

    for (int broken = 0; broken < n->numnodes; broken++) {
        if (node[broken]) {
            cmd[0] = 0;
            sprintf(c,  "ssh %s \"", n->cluster[broken]);
            strcat(cmd, c);
            for (int working = 0; working < n->numnodes; working++) {
                if (!node[working]) {
                    sprintf(c, "sudo iptables -A INPUT -s %s -p tcp --destination-port %d -j DROP -w;", n->cluster[working], n->ports[working]);
                    strcat(cmd, c);
                    sprintf(c, "sudo iptables -A INPUT -s %s -p udp --destination-port %d -j DROP -w;", n->cluster[working], n->ports[working]);
                    strcat(cmd, c);
                }
            }
            strcat(cmd, "\" < /dev/null");
            printf("%s\n", cmd);
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
        if (n->master && (n->flags & PARTITION_MASTER))
        {
            for(i=0;i<n->numnodes;i++)
            {
                if (n->master && !strcmp(n->cluster[i], n->master))
                {
                    n1 = i;
                    break;
                }
            }

            if (n1 == -1)
            {
                fprintf(stderr, "Error: couldn't find master (?)\n");
                abort();
            }
        }
        else
            n1 = rand() % n->numnodes;

        node[n1] = 1;
        do {
            n2 = rand() % n->numnodes;
        } while (n2 == n1);
        node[n2] = 1;
    }

    printf("signaling %d on ", signal);
    for (int x = 0 ; x < n->numnodes ; x++) {
        if (node[x] || all) {
            printf("%s ", n->cluster[x]);
        }
    }
    printf("\n");

    for (int broken=0 ; broken < n->numnodes ; broken++) {
        if (node[broken] || all) {
            sprintf(cmd, "ssh %s \"sudo kill -%d \\$(cat /tmp/%s.pid)\""
                    " < /dev/null\n", n->cluster[broken], signal, n->dbname);
            printf("%s", cmd);
            fflush(stdout);
            system(cmd);
        }
    }
}

void breakclocks(struct nemesis *n, int maxskew) 
{
    char cmd[1024];
    for (int x=0 ; x < n->numnodes ; x++) {
        sprintf(cmd, "ssh %s \"cur=\\$(date +%%s) ; maxskew=%d ; "
                     "newtime=\\$(( cur + (maxskew / 2) - (RANDOM %% maxskew) )) ; "
                     "sudo date \\\"+%%s\\\" -s \\@\\$newtime\" < /dev/null\n",
                     n->cluster[x], maxskew);
        printf("%s", cmd);
        system(cmd);
    }
}

void fixclocks(struct nemesis *n) 
{
    char cmd[1024];
    for (int x=0 ; x < n->numnodes ; x++) {
        sprintf(cmd, "ssh %s \"sudo service ntp stop ; sudo service ntp start\" < /dev/null\n", 
                n->cluster[x]);
        printf("%s", cmd);
        fflush(stdout);
        system(cmd);
    }
}

struct nemesis *nemesis_open(char *dbname, char *cltype, uint32_t flags)
{
    struct nemesis *n = calloc(sizeof(*n), 1);
    if (setcluster(n, dbname, cltype, flags)) {
        nemesis_close(n);
        return(NULL);
    }
    return n;
}

void nemesis_close(struct nemesis *n)
{
    if (n == NULL)
        return;
    if (n->dbname)
        free(n->dbname);
    if (n->cltype)
        free(n->cltype);
    if (n->cluster)
        free(n->cluster);
    if (n->ports)
        free(n->ports);
    if (n->master)
        free(n->master);
    free(n);
}

void fixall(struct nemesis *n)
{
    signaldb(n, SIGCONT, 1);
    fixnet(n);
    fixclocks(n);
}

