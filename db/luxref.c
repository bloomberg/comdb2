/*
   Copyright 2021, Bloomberg Finance L.P.

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

#include "plhash.h"
#include "logmsg.h"
#include "bdb_api.h"
#include "comdb2.h"
#include "luxref.h"

static int gbl_max_luxref = 0;
static hash_t *luxref_hash;

struct luxref_map_t {
    int luxref;
    struct dbtable *tbl;
};

int luxref_save_max_luxref(tran_type *tran) {
    int rc;

    if (gbl_max_luxref <= 0)
        return 0;

    rc = bdb_put_table_max_luxref(tran, gbl_max_luxref);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to store maximum luxref rc %d\n", __func__, rc);
        rc = 1;
    }

    return rc;
}

void luxref_deinit() {
    hash_clear(luxref_hash);
    hash_free(luxref_hash);
}

int luxref_init(tran_type *tran) {
    int rc;
    int is_master = !!(thedb->master == gbl_myhostname);
    int gbl_max_luxref_orig;

    if (luxref_hash) {
        luxref_deinit();
    }

    luxref_hash = hash_init_o(offsetof(struct luxref_map_t, luxref),
                              sizeof(int));

    rc = bdb_get_table_max_luxref(tran, &gbl_max_luxref);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to retrieve maximum luxref rc %d\n", __func__, rc);
        return rc;
    }
    gbl_max_luxref_orig = gbl_max_luxref;

    // Fix gbl_max_luxref if it could not be found in llmeta.
    if (gbl_max_luxref < 0) {
        gbl_max_luxref = 0;
    }

    for (int i = 0; i < thedb->num_dbs; ++i) {
        struct dbtable *tbl = thedb->dbs[i];

        // Skip if not a comdbg table
        if (!tbl->dbnum) {
            continue;
        }

        rc = bdb_get_table_luxref(tran, tbl->tablename, &tbl->luxref);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to retrieve luxref for table %s rc %d\n",
                   __func__, tbl->tablename, rc);
            return rc;
        }

        if (tbl->luxref == -1) {
            tbl->luxref = (++ gbl_max_luxref);

            if (is_master) {
                rc = bdb_put_table_luxref(tran, tbl->tablename, tbl->luxref);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR,
                           "%s: failed to store luxref for table %s (rc: %d)\n",
                           __func__, tbl->tablename, rc);
                    return rc;
                }
            }
        } else if (tbl->luxref > gbl_max_luxref) {
            gbl_max_luxref = tbl->luxref;
        }

        rc = luxref_hash_add(tbl);
        if (rc != 0) {
            return 1;
        }
    }

    if (is_master && (gbl_max_luxref_orig != gbl_max_luxref) &&
        (luxref_save_max_luxref(tran))) {
        return 1;
    }

    return 0;
}

struct dbtable *luxref_find(int luxref) {
     struct luxref_map_t *entry = hash_find(luxref_hash, (void *)&luxref);
     return (entry) ? entry->tbl : 0;
}

int luxref_hash_add(struct dbtable *tbl) {
    int rc;

    if (tbl->luxref < 0) {
        return 0;
    }

    // Update the global maximum luxref we have seen so far.
    if (tbl->luxref > gbl_max_luxref) {
        gbl_max_luxref = tbl->luxref;
    }

    if (luxref_find(tbl->luxref)) {
        return 0;
    }

    struct luxref_map_t *entry =
        (struct luxref_map_t *) malloc(sizeof(struct luxref_map_t));
    if (!entry) {
        return 1;
    }

    entry->luxref = tbl->luxref;
    entry->tbl = tbl;

    rc = hash_add(luxref_hash, entry);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to add luxref for  %s to hash (rc: %d)\n",
               __func__, tbl->tablename, rc);
        return rc;
    }

    return 0;
}

int luxref_del(tran_type *tran, struct dbtable *tbl) {
    int rc;

    if ((rc = bdb_del_table_luxref(tran, tbl->tablename)) != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to delete luxref for table %s from "
               "llmeta (rc: %d)\n", __func__, tbl->tablename, rc);
        return rc;
    }

    return 0;
}

int luxref_hash_del(struct dbtable *tbl) {
     if (!luxref_hash || !luxref_find(tbl->luxref)) {
         return 0;
     }

     return hash_del(luxref_hash, (void *)&tbl->luxref);
}

int luxref_rev_find(struct dbtable *tbl) {
    int luxref = -1;
    void *cur;
    unsigned int bkt;
    struct luxref_map_t *entry;

    entry = hash_first(luxref_hash, &cur, &bkt);
    while (entry) {
        if (entry->tbl == tbl) {
            luxref = entry->luxref;
            break;
        }
        entry = hash_next(luxref_hash, &cur, &bkt);
    }
    return luxref;
}

void luxref_dump_info() {
    void *ent;
    unsigned int bkt;
    struct luxref_map_t *entry;

    if ((hash_get_num_entries(luxref_hash)) == 0) {
        return;
    }

    logmsg(LOGMSG_USER, "{\n\t\"Tables\": [\n");
    for (entry = (struct luxref_map_t *)hash_first(luxref_hash, &ent, &bkt);
         entry;
         entry = (struct luxref_map_t *)hash_next(luxref_hash, &ent, &bkt)) {
        logmsg(LOGMSG_USER, "\t\t{\"name\": \"%s\", \"dbnum\": %d, \"luxref\": %d},\n",
               entry->tbl->tablename, entry->tbl->dbnum, entry->tbl->luxref);
    }
    logmsg(LOGMSG_USER, "{\t]\n");
    logmsg(LOGMSG_USER, "\t\"Maximum luxref\": %d\n", gbl_max_luxref);
    logmsg(LOGMSG_USER, "}\n");
}

int luxref_add(tran_type *tran, struct dbtable *tbl) {
    int rc;

    // NOOP if not a comdbg table
    if (!tbl->dbnum)
        return 0;

    rc = bdb_get_table_luxref(tran, tbl->tablename, &tbl->luxref);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to retrieve luxref for table %s rc %d\n",
               __func__, tbl->tablename, rc);
        return rc;
    }

    if (tbl->luxref == -1) {
        tbl->luxref = (++ gbl_max_luxref);

        rc = bdb_put_table_luxref(tran, tbl->tablename, tbl->luxref);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to store luxref for table %s (rc: %d)\n",
                   __func__, tbl->tablename, rc);
            return rc;
        }
    } else if (tbl->luxref > gbl_max_luxref) {
        gbl_max_luxref = tbl->luxref;
    }

    return luxref_save_max_luxref(tran);
}
