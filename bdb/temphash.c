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
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <build/db.h> /* berk db.h */
#include <net.h>
#include <sbuf2.h>
#include "bdb_int.h"
#include <list.h>

#include <logmsg.h>

struct bdb_temp_hash {
    DB *db;
    bdb_state_type *state;
    char *filename;
};

struct bdb_temp_hash *bdb_temp_hash_create_cache(bdb_state_type *state,
                                                 int cacheszkb, char *tmpname,
                                                 int *bdberr)
{
    struct bdb_temp_hash *h = malloc(sizeof(struct bdb_temp_hash));
    int rc;
    unsigned int gb = 0, bytes = 0;

    *bdberr = BDBERR_NOERROR;

    h->filename = strdup(tmpname);
    h->state = state;
    rc = db_create(&h->db, NULL, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "bdb_temp_hash_create:db_create rc %d\n", rc);
        free(h->filename);
        free(h);
        *bdberr = rc;
        return NULL;
    }

    if (cacheszkb > 0) {
        /* 20kb is too little - Richard Possnett's constraints kill it
         * pretty easily */
        if (cacheszkb < 256)
            cacheszkb = 256;
        gb = (cacheszkb / 1000000);
        bytes = (cacheszkb % 1000000) * 1000;
        rc = h->db->set_cachesize(h->db, gb, bytes, 0);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "invalid %s set_cache_size call: gb %d bytes %d\n", tmpname,
                   gb, bytes);
            free(h->filename);
            free(h);
            *bdberr = rc;
            return NULL;
        }
    }

    rc = h->db->open(h->db, NULL, h->filename, NULL, DB_HASH,
                     DB_CREATE | DB_TRUNCATE, 0666);
    if (rc) {
        logmsg(LOGMSG_ERROR, "bdb_temp_hash_create:open rc %d\n", rc);
        free(h->filename);
        free(h);
        *bdberr = rc;
        return NULL;
    }

    return h;
}

struct bdb_temp_hash *bdb_temp_hash_create(bdb_state_type *state, char *tmpname,
                                           int *bdberr)
{
    return bdb_temp_hash_create_cache(state, 0, tmpname, bdberr);
}

int bdb_temp_hash_destroy(struct bdb_temp_hash *h)
{
    int rc;
    int outrc = 0;
    rc = h->db->close(h->db, DB_NOSYNC); /* don't care if it ever makes it to
                                     disk since the db is about to be removed */

    if (rc) {
        outrc = rc;
        logmsg(LOGMSG_ERROR, "bdb_temp_hash_destroy:close rc %d\n", rc);
    }
    rc = unlink(h->filename);
    if (rc) {
        outrc = rc;
        logmsg(LOGMSG_ERROR, "bdb_temp_hash_destroy:unlink rc %d\n", rc);
    }
    free(h->filename);
    free(h);
    return outrc;
}

int bdb_temp_hash_insert(struct bdb_temp_hash *h, void *key, int keylen,
                         void *dta, int dtalen)
{
    DBT dkey, ddata;

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));

    dkey.data = key;
    dkey.size = keylen;
    ddata.data = dta;
    ddata.size = dtalen;
    return h->db->put(h->db, NULL, &dkey, &ddata, 0);
}

int bdb_temp_hash_lookup(struct bdb_temp_hash *h, void *key, int keylen,
                         void *dta, int *dtalen, int maxlen)
{
    DBT dkey, ddata;
    int rc;

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.data = key;
    dkey.size = keylen;
    ddata.flags = DB_DBT_USERMEM;
    ddata.ulen = maxlen;
    ddata.data = dta;

    rc = h->db->get(h->db, NULL, &dkey, &ddata, 0);
    *dtalen = ddata.size;
    return rc;
}
