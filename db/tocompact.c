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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>

#include <str0.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"

#ifdef BERKDB_46

static void *compact_my_table(void *arg);

/* allow only one compaction per table at a time*/
static pthread_mutex_t compact_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct compact_entry {
    struct dbtable *db;
    int timeout;
    int freefs;
} compact_entry_t;

void compact_table(struct dbtable *db, int timeout, int freefs)
{

    pthread_t tid;
    int rc;
    compact_entry_t *ce = (compact_entry_t *)malloc(sizeof(compact_entry_t));

    if (!ce) {
        fprintf(stderr, "compact_table: not enough memory for a "
                        "compact_entry_t. this system is busted.\n");
        return;
    }

    /* check to see if there's another compaction in progress, fail if so*/
    rc = pthread_mutex_trylock(&compact_mutex);
    if (rc) {
        fprintf(stderr, "A compaction is already in progress for %s: %d %s\n",
                db->dbname, rc, strerror(rc));
        free(ce);
        return;
    }

    ce->db = db;
    ce->timeout = timeout;
    ce->freefs = freefs;

    /* create thread that will take care of compaction*/
    rc = pthread_create(&tid, &gbl_pthread_attr_detached, compact_my_table, ce);
    if (rc) {
        fprintf(stderr, "cannot spawn compaction thread for %s: %d %s\n",
                db->dbname, rc, strerror(rc));
        free(ce);
        return;
    }
}

/* THIS IS COMPACTION THREAD*/
static void *compact_my_table(void *arg)
{

    compact_entry_t *ce = (compact_entry_t *)arg;
    int rc = 0;

    thread_started("compact table");

    if (!ce || !ce->db) {
        printf("compact_thread: no db provided!\n");
        return NULL;
    }

    /* compact this db */
    backend_thread_event(ce->db->dbenv, COMDB2_THR_EVENT_START_RDONLY);
    rc = compact_db(ce->db, ce->timeout, ce->freefs);
    backend_thread_event(ce->db->dbenv, COMDB2_THR_EVENT_DONE_RDONLY);
    if (rc) {
        printf("compaction for table %s failed: %d\n", ce->db->dbname, rc);
    } else {
        printf("compaction for table %s succeded\n", ce->db->dbname);
    }

    free(ce);

    /* release the mutex*/
    pthread_mutex_unlock(&compact_mutex);

    return NULL;
}
#endif
