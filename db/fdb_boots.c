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

extern int gbl_myroom;

struct fdb_location {
    char *dbname;          /* which fdb */
    enum mach_class class; /* class cluster: prod, alpha, beta,...*/

    int nnodes;
    char **nodes; /* bb node numbers */
    char *lcl;    /* set for same datacenter nodes */

    int need_refresh; /* if a previous run returned error, refresh the nodes */
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
                                     int *nnodes, int room[REPMAX]);

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

    lvl = get_class_str(loc->class);
    if (strncasecmp(lvl, "???", 3) == 0) {
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
        loc->nodes = (char **)realloc(loc->nodes, sizeof(char **) * nnodes);
        if (!loc->nodes) {
            return FDB_ERR_MALLOC;
        }
        loc->lcl = (char *)realloc(loc->lcl, sizeof(char *) * nnodes);
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
 * Retrieve the nodes from codmb2db
 */
int fdb_locate(const char *dbname, enum mach_class class, int refresh,
               fdb_location_t **ploc)
{
    fdb_location_t *loc = *ploc;
    int rc = 0;

    if (!loc || refresh) {
        if (!loc) {
            loc = (fdb_location_t *)calloc(1, sizeof(fdb_location_t) +
                                                  strlen(dbname) + 1);
            if (!loc) {
                return FDB_ERR_MALLOC;
            }
            loc->dbname = ((char *)loc) + sizeof(fdb_location_t);
            snprintf(loc->dbname, strlen(dbname) + 1, "%s", dbname);
            loc->class = class;
        } else if (loc && refresh) {
            /* free location */
            if (loc->nodes) {
                bzero(loc->nodes, sizeof(loc->nnodes));
                bzero(loc->lcl, sizeof(loc->nodes));
            }
            loc->nnodes = 0;
            loc->need_refresh = 0;
        }

        /* get the nodes */
        rc = _fdb_refresh_location(dbname, loc);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to retrieve %s nodes\n", __func__,
                    dbname);
        }

        *ploc = loc;
    }

    return rc;
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
                      int *avail_nodes, int *p_lcl_nodes)
{
    fdb_location_t *loc = *ploc;
    int rc = 0;
    char *lcl_nodes[REPMAX];
    int lcl_nnodes;
    char *rescpu_nodes[REPMAX];
    int rescpu_nnodes = 0;
    char *host = NULL;
    int arg_idx;
    int i;
    int masked_op;
    int prefer_local;

    /* if we are here, and we don't have a location already, it means this was
       local */
    if (!loc) {
        if (avail_nodes) {
            *avail_nodes = 1;
        }

        if (p_lcl_nodes) {
            *p_lcl_nodes = 1;
        }

        return gbl_mynode; /* local node */
    }

    assert(loc != NULL);

    if ((op & FDB_LOCATION_REFRESH) || loc->need_refresh) {
        rc = fdb_locate(loc->dbname, loc->class, loc->need_refresh, ploc);
        if (rc) {
            goto done;
        }
    }

    masked_op = op & (FDB_LOCATION_INITIAL | FDB_LOCATION_NEXT);

    switch (masked_op) {
    case FDB_LOCATION_INITIAL:

        /* routing; ignoring down nodes */
        lcl_nnodes = 0;
        rescpu_nnodes = 0;
        for (i = 0; i < loc->nnodes; i++) {
            if (!machine_is_up(loc->nodes[i])) {
                continue;
            }

            /* prefer local node if available */
            if (loc->nodes[i] == gbl_mynode) {
                host = loc->nodes[i];
            }

            /* prefer same datacenter if available */
            if (!(op & FDB_LOCATION_IGNORE_LCL) && loc->lcl[i]) {
                lcl_nodes[lcl_nnodes++] = loc->nodes[i];
            }

            /* all the rescpu pool */
            rescpu_nodes[rescpu_nnodes++] = loc->nodes[i];
        }

        if (host == NULL) {
            if (rescpu_nnodes <= 0) {
                rc = FDB_ERR_REGISTER_NORESCPU;
                goto done;
            }

            /* random selection out of same datacenter */
            if (lcl_nnodes > 0) {
                host = lcl_nodes[random() % lcl_nnodes];
            }
            /* no same datacenter, get a random one */
            else {
                host = rescpu_nodes[random() % rescpu_nnodes];
            }
        }
        break;

    case FDB_LOCATION_NEXT:

        lcl_nnodes = 0;
        rescpu_nnodes = 0;
        arg_idx = -1;

        /* if we are single node, return error */
        if (loc->nnodes == 1 && (loc->nodes[0] == arg)) {
            rc = FDB_ERR_REGISTER_NONODES;
            goto done;
        }

        /* look for current node */
        for (i = 0; i < loc->nnodes; i++) {
            if (loc->nodes[i] == arg) {
                arg_idx = i;
                break;
            }
        }

        /* our current dissapear? hm, start with the first known node */
        if (arg_idx == -1)
            arg_idx = 0;

        /* try to find a local node following the one I am trying and grab any
         * rescpu one that is local, if any */
        prefer_local = !(op & FDB_LOCATION_IGNORE_LCL);
        do {
            i = arg_idx;
            rescpu_nnodes = 0;
            lcl_nnodes = 0;
            do {
                i = (i + 1) % loc->nnodes;

                if (i == arg_idx)
                    break;

                /* ignore rtcpu */
                if (!machine_is_up(loc->nodes[i])) {
                    continue;
                }

                rescpu_nnodes++;
                if (loc->lcl[i])
                    lcl_nnodes++;

                if (host == NULL) {
                    /* ignore non locals first run */
                    if (prefer_local && loc->lcl[i]) {
                        host = loc->nodes[i];
                    } else if (!prefer_local) {
                        /* we want ANY node */
                        host = loc->nodes[i];
                    }
                }
            } while (i != arg_idx);

            /* found our rescpu local? */
            if (host) {
                break;
            }

            /* we did not find a node */
            if (prefer_local) {
                /* try to look into other datacenters */
                prefer_local = 0;
            } else {
                break; /* done */
            }
        } while (1);

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
    return host;
}

static int _discover_remote_db_nodes(const char *dbname, const char *class,
                                     int maxnodes,
                                     /* out: */ char *nodes[REPMAX],
                                     int *nnodes, int *room)
{
    char query[1024];
    int rc = FDB_NOERR;
    char *node;
    cdb2_hndl_tp *db;

    /* NOTE: test is dev */
    if (strncasecmp(class, "test", 4) == 0) {
        class = "dev";
    }

    rc = cdb2_open(&db, "comdb2db", "default", 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: can't talk to metadb rc %d %s\n", __func__, rc,
                cdb2_errstr(db));
        cdb2_close(db);
        return FDB_ERR_GENERIC;
    }

    /* get the nodes on which a db runs, rescpued */
    snprintf(query, sizeof(query),
             "select m.name, m.room  from machines as m,clusters as c, "
             "databases as d"
             " where c.name=@dbname and c.cluster_name=@class"
             " and m.cluster=c.cluster_machs and d.name=@dbname");

    rc = cdb2_bind_param(db, "dbname", CDB2_CSTRING, dbname, strlen(dbname));
    if (rc) {
        logmsg(LOGMSG_ERROR, "bind dbname rc %d %s\n", rc, cdb2_errstr(db));
        return FDB_ERR_GENERIC;
    }
    rc = cdb2_bind_param(db, "class", CDB2_CSTRING, class, strlen(class));
    if (rc) {
        logmsg(LOGMSG_ERROR, "bind class rc %d %s\n", rc, cdb2_errstr(db));
        return FDB_ERR_GENERIC;
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
                if (strncasecmp(cdb2_column_value(db, 1), "NY", 2) == 0) {
                    room[*nnodes] = 1;
                } else {
                    room[*nnodes] = 2;
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

#if 0
/**
 * Set the affinity node; setting it to -1 will force a refresh
 *
 */
int fdb_aff_set_node(fdb_affinity_t *aff)
{
   return 0;
}

int fdb_aff_get_node(fdb_affinity_t *aff)
{
   return 0;
}

int fdb_aff_destroy(fdb_affinity_t **paff)
{
}

int fdb_get_next_node(fdb_location_t *loc, int datacenter_affinity, int crt_node);
#endif

