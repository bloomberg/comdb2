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

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <limits.h>

#include "cdb2_constants.h"
#define MAXCACHE 32

#ifdef DEBUG_NODEMAP
#define MAXCACHE 10
#else
#define MAXCACHE 32
#endif

#include "nodemap.h"
#include "intern_strings.h"

#include <logmsg.h>

struct node_index {
    const char *node;
    int ref;
    int ix;
};

static int numnodes;
static struct node_index nodes[MAXNODES];
static pthread_mutex_t lk;

volatile static __thread int lnumnodes;
volatile static __thread struct node_index lnodes[MAXCACHE];

/* We used to index by node number.  Would like to continue indexing by node
 * number,
 * but there's no longer such an entity.  So we need a lookup structure to
 * reduce
 * an name to a small index so we can size things by it.  Don't want to lock.
 * So cheat slightly by keeping a small lookaside list of nodes.  Their count
 * will likely
 * remain small.  If we don't find one search the global list.  No need to lock.
 * When there's
 * a change to the global list, clear the local cache.  */
static int nodeix_global(const char *node)
{
    if (!isinterned(node))
        abort();

    for (int i = 0; i < numnodes; i++) {
        if (nodes[i].node == node) {
            if (lnumnodes < MAXCACHE) {
#ifdef DEBUG_NODEMAP
                printf("added local, have enough room\n");
#endif
                lnodes[lnumnodes].node = node;
                lnodes[lnumnodes].ix = i;
                lnodes[lnumnodes].ref = 1;
                lnumnodes++;
                return i;
            } else {
                int minref = INT_MAX;
                int minix = 0;
                for (int j = 0; j < lnumnodes; j++) {
                    if (lnodes[j].ref < minref) {
                        minref = lnodes[j].ref;
                        minix = j;
                    }
                }
#ifdef DEBUG_NODEMAP
                printf("added local, evicted node %d ix %d (lix %d) ref %d\n",
                       lnodes[minix].node, lnodes[minix].ix, minix,
                       lnodes[minix].ref);
#endif
                lnodes[minix] = nodes[i];
                lnodes[minix].ref = 1;
                return i;
            }
        }
    }

    /* didn't find in global list, add - now lock*/
    pthread_mutex_lock(&lk);
    /* repeat the search in case someone else inserted it */
    for (int i = 0; i < numnodes; i++) {
        if (nodes[i].node == node) {
            /* someone else inserted it, return the index */
            pthread_mutex_unlock(&lk);
            return i;
        }
    }
    /* we get to insert it in the next available slot */
    if (numnodes == MAXNODES) {
        logmsg(LOGMSG_FATAL, "too many nodes in list: %d\n", numnodes);
        /* TODO: resize, make search above happen under lock? */
        abort();
    }
    nodes[numnodes].node = node;
    nodes[numnodes].ix = numnodes;
    nodes[numnodes].ref = 0;
    numnodes++;
    pthread_mutex_unlock(&lk);

    /* next search will add to local */
    // printf("added global %s->%d\n", node, numnodes-1);
    return numnodes - 1;
}

int nodeix(const char *node)
{
    int ix = -1;
    for (int i = 0; i < lnumnodes; i++) {
        if (lnodes[i].node == node) {
            lnodes[i].ref++;
            // printf("found local %s->%d\n", node, lnodes[i].ix);
            return lnodes[i].ix;
        }
    }
    ix = nodeix_global(node);
    // printf("found global %s->%d\n", node, ix);
    return ix;
}

const char *nodeat(int ix)
{
    if (ix < 0 || ix >= MAXNODES)
        return 0;
    return nodes[ix].node;
}
