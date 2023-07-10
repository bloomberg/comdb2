/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <stdio.h>
#include <assert.h>

#include <rtcpu.h>

#include "comdb2.h"
#include "fdb_boots.h"
#include "intern_strings.h"
#include "cdb2api.h"
#include "sql.h"
#include "fdb_fend.h"
#include "logmsg.h"
#include "locks_wrap.h"

#define MY_CLUSTER_MAX 32

extern int gbl_myroom;

struct fdb_location {
    char *dbname;          /* which fdb */
    enum mach_class class; /* class cluster: prod, alpha, beta,...*/

    int nnodes;
    char **nodes; /* bb node numbers */
    int *lcl;    /* set for same datacenter nodes */
};

struct fdb_affinity_elem {
    char *dbname; /* name of foreign db */
    int node;     /* "sticky" node value; changing on error,
                     -1 means need value
                     0 means local node
                   */
};
typedef struct fdb_affinity_elem fdb_affinity_elem_t;

struct fdb_affinity {
    int n_used_nodes;
    int n_alloc_nodex;
    fdb_affinity_elem_t *ents;
};

static int _discover_remote_db_nodes(const char *dbname, const char *class,
                                     int maxnodes,
                                     /* out: */ char *nodes[REPMAX],
                                     int *nnodes, int *room);

void _extract_cdb2api_metadb_info(char *info, char ** name, char **class);

/**
 * Retrieves node location from comdb2db for "dbname" of class "class"
 *
 * NOTE: fdb is dbcon_mtx locked here
 */
static int _fdb_refresh_location(const char *dbname, fdb_location_t *loc)
{
    char *nodes[REPMAX];
    int room[REPMAX];
    int nnodes;
    int i;
    int my_room = (gbl_myroom == 5) ? 2 : gbl_myroom;
    int rc;
    const char *lvl = NULL;

    assert(loc);

    lvl = mach_class_class2tier(loc->class);
    if (strncasecmp(lvl, "unknown", 7) == 0) {
        return FDB_ERR_CLASS_UNKNOWN;
    }

    /* retrieve nodes */
    nnodes = 0;
    rc = _discover_remote_db_nodes(dbname, lvl, REPMAX, nodes, &nnodes, room);
    if (rc != FDB_NOERR || nnodes <= 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to retrieve %s (%s) nodes rc=%d nnodes=%d\n",
                __func__, dbname, lvl, rc, nnodes);
        return rc ? rc : FDB_ERR_REGISTER_NONODES;
    }

    /* cache the nodes locally */
    if (loc->nnodes != nnodes) {
        loc->nodes = (char **)realloc(loc->nodes, sizeof(char *) * nnodes);
        if (!loc->nodes) {
            return FDB_ERR_MALLOC;
        }
        loc->lcl = (int *)realloc(loc->lcl, sizeof(int) * nnodes);
        if (!loc->lcl) {
            return FDB_ERR_MALLOC;
        }
    }

    loc->nnodes = nnodes;
    memcpy(loc->nodes, nodes, sizeof(char *) * nnodes);

    /* mark same datacenter nodes */
    for (i = 0; i < loc->nnodes; i++) {
        loc->lcl[i] = (my_room == room[i]);
    }

    return FDB_NOERR;
}

/**
 * Locate a certain database
 *
 * Retrieve the nodes from comdb2db
 */
int fdb_locate(const char *dbname, enum mach_class class, int refresh,
               fdb_location_t **ploc, pthread_mutex_t *mtx)
{
    fdb_location_t *loc = *ploc;
    int rc = 0;

    Pthread_mutex_lock(mtx);
    if (!loc || refresh) {
        if (!loc) {
            loc = (fdb_location_t *)calloc(1, sizeof(fdb_location_t) +
                                                  strlen(dbname) + 1);
            if (!loc) {
                Pthread_mutex_unlock(mtx);
                return FDB_ERR_MALLOC;
            }
            loc->dbname = ((char *)loc) + sizeof(fdb_location_t);
            snprintf(loc->dbname, strlen(dbname) + 1, "%s", dbname);
            loc->class = class;
        } else if (loc && refresh) {
            /* free location */
            if (loc->nodes) {
                bzero(loc->nodes, loc->nnodes * sizeof(loc->nodes[0]));
                int i;
                for (i = 0; i < loc->nnodes; i++)
                    loc->lcl[i] = 0;
            }
            loc->nnodes = 0;
        }

        /* get the nodes */
        rc = _fdb_refresh_location(dbname, loc);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to retrieve %s nodes\n", __func__,
                    dbname);
        }

        *ploc = loc;
    }
    Pthread_mutex_unlock(mtx);

    return rc;
}

static char *_get_node_initial(int nnodes, char **nodes, int *lcl,
                               enum fdb_location_op op, int *lcl_nnodes,
                               int *rescpu_nnodes)
{
    char *lcl_nodes[MY_CLUSTER_MAX];
    char *rescpu_nodes[MY_CLUSTER_MAX];
    char *node = NULL;
    int i;

    /* routing; ignoring down nodes */
    *lcl_nnodes = 0;
    *rescpu_nnodes = 0;
    for (i = 0; i < nnodes; i++) {
        if (!machine_is_up(nodes[i])) {
            continue;
        }

        /* prefer local node if available */
        if (nodes[i] == gbl_myhostname) {
            node = nodes[i];
        }

        /* prefer same datacenter if available */
        if (!(op & FDB_LOCATION_IGNORE_LCL) && lcl[i]) {
            lcl_nodes[(*lcl_nnodes)++] = nodes[i];
        }

        /* all the rescpu pool */
        rescpu_nodes[(*rescpu_nnodes)++] = nodes[i];
    }

    if (!node) {
        /* if local node is not part of remote cluster, randomly picked a
           node, prefering the local dc */
        if (*rescpu_nnodes <= 0)
            return NULL;

        /* random selection out of same datacenter */
        if (*lcl_nnodes > 0) {
            node = lcl_nodes[random() % *lcl_nnodes];
        }
        /* no same datacenter, get a random one */
        else {
            node = rescpu_nodes[random() % *rescpu_nnodes];
        }
    }

    /* we got a good node */
    return node;
}

static char *_get_node_next(int nnodes, char **nodes, int *lcl, char *arg,
                            enum fdb_location_op op, int *lcl_nnodes,
                            int *rescpu_nnodes)
{
    int arg_idx;
    int prefer_local;
    int i;
    char *node = NULL;

    *lcl_nnodes = 0;
    *rescpu_nnodes = 0;

    /* if we are single node, return error */
    if (nnodes == 1 && (nodes[0] == arg))
        return NULL;

    /* look for current node */
    arg_idx = 0; /* if node dissapear, stick with first node */
    for (i = 0; i < nnodes; i++) {
        if (nodes[i] == arg) {
            arg_idx = i;
            break;
        }
    }

    /* try to find a local node following the one I am trying and
       grab any rescpu one that is local, if any */
    prefer_local = !(op & FDB_LOCATION_IGNORE_LCL);
    do {
        i = arg_idx;
        *rescpu_nnodes = 0;
        *lcl_nnodes = 0;
        do {
            i = (i + 1) % nnodes;

            if (i == arg_idx)
                break;

            /* ignore rtcpu */
            if (!machine_is_up(nodes[i])) {
                continue;
            }

            (*rescpu_nnodes)++;
            if (lcl[i])
                (*lcl_nnodes)++;

            if (node == NULL) {
                /* ignore non locals first run */
                if (prefer_local && lcl[i]) {
                    node = nodes[i];
                } else if (!prefer_local) {
                    /* we want ANY node */
                    node = nodes[i];
                }
            }
        } while (i != arg_idx);

        /* found our rescpu local? */
        if (node) {
            break;
        }

        /* if tried all nodes, or none rescpu, error out */
        if (!prefer_local || !(*rescpu_nnodes)) {
            return NULL;
        }

        /* try to look into other datacenters */
        prefer_local = 0;
    } while (1);

    return node;
}

/**
 * Routing algo
 *
 * Actions:
 * INITIAL = an intial node
 * NEXT = next node after "arg" initial
 *
 *  Modifiers
 * REFRESH will query comdb2db again
 * IGNORE_LCL will not prefer datacenter colocated nodes
 *
 *
 */
char *fdb_select_node(fdb_location_t **ploc, enum fdb_location_op op, char *arg,
                      int *avail_nodes, int *p_lcl_nodes, pthread_mutex_t *mtx)
{
    fdb_location_t *loc = *ploc;
    int rc = 0;
    int lcl_nnodes;
    int rescpu_nnodes = 0;
    char *host = NULL;
    int masked_op = 0;
    int my_nnodes = 0;
    char **my_nodes = NULL;
    int *my_lcl = NULL;

    /* if we are here, and we don't have a location already, it means this was
       local */
    if (!loc) {
        if (avail_nodes) {
            *avail_nodes = 1;
        }

        if (p_lcl_nodes) {
            *p_lcl_nodes = 1;
        }

        return gbl_myhostname; /* local node */
    }

    assert(loc != NULL);

    if (op & FDB_LOCATION_REFRESH) {
    retry:
        rc = fdb_locate(loc->dbname, loc->class, 1, ploc, mtx);
        if (rc) {
            goto done;
        }
    }

    masked_op = op & (FDB_LOCATION_INITIAL | FDB_LOCATION_NEXT);

    Pthread_mutex_lock(mtx);
    /* cache information locally, in case the remote is all down
       and multiple threads might try to refresh the information;
       this is optimistic and better and a lock */
    my_nnodes = loc->nnodes;
    my_nodes = realloc(my_nodes, loc->nnodes * sizeof(loc->nodes[0]));
    my_lcl = realloc(my_lcl, loc->nnodes * sizeof(loc->lcl[0]));
    memcpy(my_nodes, loc->nodes, my_nnodes * sizeof(loc->nodes[0]));
    memcpy(my_lcl, loc->lcl, my_nnodes * sizeof(loc->lcl[0]));
    if (my_nnodes == 0) {
        Pthread_mutex_unlock(mtx);
        goto retry;
    }
    Pthread_mutex_unlock(mtx);

    switch (masked_op) {
    case FDB_LOCATION_INITIAL:
        host = _get_node_initial(my_nnodes, my_nodes, my_lcl, op, &lcl_nnodes,
                                 &rescpu_nnodes);
        break;

    case FDB_LOCATION_NEXT:
        host = _get_node_next(my_nnodes, my_nodes, my_lcl, arg, op, &lcl_nnodes,
                              &rescpu_nnodes);
        break;
    }

    if (host == NULL) {
        logmsg(LOGMSG_ERROR, "%s: unable to find node for %s\n", __func__,
                loc->dbname);
        rc = FDB_ERR_REGISTER_NORESCPU;
        goto done;
    }

    if (avail_nodes) {
        *avail_nodes = rescpu_nnodes;
    }

    if (p_lcl_nodes) {
        *p_lcl_nodes = lcl_nnodes;
    }

done:
    if (my_lcl)
        free(my_lcl);
    if (my_nodes)
        free(my_nodes);

    if (gbl_fdb_track)
        logmsg(LOGMSG_INFO,
               "!!! USING %s op %d masked %d\n",
               host, op, masked_op);
    return host;
}

char *gbl_foreign_metadb = NULL;
char *gbl_foreign_metadb_class = NULL;
char *gbl_foreign_metadb_config = NULL;
static int _discover_remote_db_nodes(const char *dbname, const char *class,
                                     int maxnodes,
                                     /* out: */ char *nodes[REPMAX],
                                     int *nnodes, int *room)
{
    int rc = FDB_NOERR;
    char *node;
    cdb2_hndl_tp *db;
    char *comdb2dbname = NULL;
    char *comdb2dbclass = NULL;

    if (gbl_foreign_metadb && gbl_foreign_metadb_class) {
        comdb2dbname = strdup(gbl_foreign_metadb);
        comdb2dbclass = strdup(gbl_foreign_metadb_class);
    } else if (gbl_foreign_metadb_config) {
        char *dupstr = strdup(gbl_foreign_metadb_config);
        _extract_cdb2api_metadb_info(dupstr, &comdb2dbname, &comdb2dbclass);
        free(dupstr);
    } else {
        /* NOTE: test is dev */
        if ((strncasecmp(class, "test", 4) == 0) ||
                (strncasecmp(class, "dev", 3) == 0) ||
		(strncasecmp(class, "fuzz", 4) == 0)) {
            class = (strncasecmp(class, "fuzz", 4) == 0) ? "fuzz" : "dev";
            comdb2dbname = strdup("comdb3db");
            comdb2dbclass = strdup("dev");
        } else {
            comdb2dbname = strdup("comdb2db");
            comdb2dbclass = strdup("prod");
        }
    }

    rc = cdb2_open(&db, comdb2dbname, comdb2dbclass, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't talk to metadb rc %d %s\n", __func__, rc,
                cdb2_errstr(db));
        rc = FDB_ERR_GENERIC;
        goto noclose_done;
    }
    if (gbl_fdb_track)
        logmsg(LOGMSG_INFO, "FDB: for %s, connected to metadb %s:%s\n",
               dbname, comdb2dbname, comdb2dbclass);

    /* get the nodes on which a db runs, rescpued */
    const char *query = "select m.name, m.room  from machines as m,clusters as "
                        "c, databases as d where c.name=@dbname and "
                        "c.cluster_name=@class and m.cluster=c.cluster_machs "
                        "and d.name=@dbname";

    rc = cdb2_bind_param(db, "dbname", CDB2_CSTRING, dbname, strlen(dbname));
    if (rc) {
        logmsg(LOGMSG_ERROR, "bind dbname rc %d %s\n", rc, cdb2_errstr(db));
        rc = FDB_ERR_GENERIC;
        goto done;
    }
    rc = cdb2_bind_param(db, "class", CDB2_CSTRING, class, strlen(class));
    if (rc) {
        logmsg(LOGMSG_ERROR, "bind class rc %d %s\n", rc, cdb2_errstr(db));
        rc = FDB_ERR_GENERIC;
        goto done;
    }

    rc = cdb2_run_statement(db, query);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed run query comdb2db rc=%d %s\n", __func__,
                rc, cdb2_errstr(db));
        rc = FDB_ERR_GENERIC;
        goto done;
    }

    *nnodes = 0;
    do {
        rc = cdb2_next_record(db);
        if (rc == CDB2_OK) {
            switch (cdb2_column_type(db, 0)) {
            case CDB2_CSTRING: {
                node = intern(cdb2_column_value(db, 0));
                if (gbl_fdb_track)
                    logmsg(LOGMSG_INFO, "FDB: for %s found node %s\n",
                           dbname, node);
            } break;
            default:
                logmsg(LOGMSG_ERROR, "%s: comdb2db returned funny type for column name\n",
                        __func__);
                rc = FDB_ERR_GENERIC;
                goto done;
            }
#if 0 
         printf("%s: Got node %d for %s, having %d nodes\n", __func__,
               node, dbname, *nnodes);
#endif

            if (*nnodes < maxnodes - 1) {
                nodes[*nnodes] = node;

                assert(cdb2_column_type(db, 1) == CDB2_CSTRING);
                if (strncasecmp(cdb2_column_value(db, 1), "ORG", 3) == 0) {
                    room[*nnodes] = 6;
                } else if (strncasecmp(cdb2_column_value(db, 1), "NJ", 2) ==
                           0) {
                    room[*nnodes] = 2;
                } else {
                    room[*nnodes] = 1;
                }

                (*nnodes)++;
            } else {
                logmsg(LOGMSG_ERROR, "%s: cluster is too large %d > max %d, ignoring\n",
                        __func__, (*nnodes) + 1, maxnodes);
            }
        }
    } while (rc == CDB2_OK);

    if (rc == CDB2_OK_DONE)
        rc = FDB_NOERR;

done:
    cdb2_close(db);
noclose_done:
    free(comdb2dbname);
    free(comdb2dbclass);
    return rc;
}

/**
 * Get the number of available/rescpu nodes
 *
 */
int fdb_get_rescpu_nodes(fdb_location_t *loc, int *locals)
{
    int i;
    int rescpued;

    /* this is local */
    if (!loc) {
        /* we have one node, the local */
        if (locals)
            *locals = 1;

        return 1;
    }

    if (locals)
        *locals = 0;

    rescpued = 0;
    for (i = 0; i < loc->nnodes; i++) {
        if (machine_is_up(loc->nodes[i])) {
            rescpued++;

            if (loc->lcl[i] && locals)
                (*locals)++;
        }
    }

    return rescpued;
}

static int read_line(char *line, int nline, const char *buf, int *offset)
{
    int off = *offset;
    int ch = buf[off];
    while (ch == ' ' || ch == '\n') {
        ch = buf[++off];
    }

    int count = 0;
    while ((ch != '\n') && (ch != EOF) && (ch != '\0')) {
        line[count] = ch;
        count++;
        if (count >= nline)
            return count;
        ch = buf[++off];
    }
    *offset = off;
    if (count == 0)
        return -1;
    line[count + 1] = '\0';
    return count + 1;
}

void _extract_cdb2api_metadb_info(char *info, char ** name, char **class)
{
    char line[PATH_MAX > 2048 ? PATH_MAX : 2048] = {0};
    int offset = 0;

    *name = *class = NULL;
    while (read_line(line, sizeof(line), info, &offset) != -1) {
        char *last = NULL;
        char *tok = NULL;
        tok = strtok_r(line, " :", &last);
        if (tok == NULL) {
            continue;
        } else if (strcasecmp("comdb2_config", tok) == 0) {
            tok = strtok_r(NULL, " =:,", &last);
            if (tok == NULL) continue;
            if (strcasecmp("default_type", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    *class = strdup(tok);
            } else if (strcasecmp("comdb2dbname", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    *name = strdup(tok);
            }
        }
        bzero(line, sizeof(line));
    }
}

