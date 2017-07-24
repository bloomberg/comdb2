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

#include "schemachange.h"
#include "sc_util.h"
#include "logmsg.h"

int close_all_dbs(void)
{
    int ii, rc, bdberr;
    struct dbtable *db;
    logmsg(LOGMSG_DEBUG, "Closing all tables...\n");
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        rc = bdb_close_only(db->handle, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "failed closing table '%s': %d\n", db->dbname,
                   bdberr);
            return -1;
        }
    }
    logmsg(LOGMSG_DEBUG, "Closed all tables OK\n");
    return 0;
}

int open_all_dbs(void)
{
    int ii, rc, bdberr;
    struct dbtable *db;
    logmsg(LOGMSG_DEBUG, "Opening all tables\n");
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        rc = bdb_open_again(db->handle, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR,
                   "morestripe: failed reopening table '%s': %d\n", db->dbname,
                   bdberr);
            return -1;
        }
    }
    logmsg(LOGMSG_DEBUG, "Opened all tables OK\n");
    gbl_sc_commit_count++;
    return 0;
}

/* Check if it is, in prinicple, ok to start a schema change. */
int check_sc_ok(struct schema_change_type *s)
{
    /* I must be rtcpu up */
    if (!is_node_up(gbl_mynode)) {
        sc_errf(s, "cannot perform schema change; I am rtcpu'd down\n");
        return -1;
    }

    /* My sanc list must be intact */
    if (!net_sanctioned_list_ok(thedb->handle_sibling)) {
        sc_errf(s, "cannot perform schema change; sanctioned list nodes are "
                   "missing\n");
        return -1;
    }

    /* ok */
    return 0;
}

int llmeta_get_dbnum_tran(void *tran, char *tablename, int *bdberr)
{
    int rc;
    int dbnums[MAX_NUM_TABLES];
    char *tblnames[MAX_NUM_TABLES];
    int numtbls;
    int i;
    int dbnum = -1;

    rc = bdb_llmeta_get_tables(tran, (char **)&tblnames, dbnums, MAX_NUM_TABLES,
                               &numtbls, bdberr);
    if (rc) {
        /* TODO: errors */
        logmsg(LOGMSG_ERROR, "%s:%d bdb_llmeta_get_tables rc %d bdberr %d\n",
               __FILE__, __LINE__, rc, bdberr);
        return rc;
    }
    for (i = 0; i < numtbls; i++) {
        if (strcasecmp(tblnames[i], tablename) == 0) {
            dbnum = dbnums[i];
        }
        free(tblnames[i]);
    }
    return dbnum;
}

int llmeta_get_dbnum(char *tablename, int *bdberr)
{
    return llmeta_get_dbnum_tran(NULL, tablename, bdberr);
}

/* careful this can cause overflows, do not use */
char *get_temp_db_name(struct dbtable *db, char *prefix, char tmpname[])
{
    sprintf(tmpname, "%s%s", prefix, db->dbname);

    return tmpname;
}

// get offset of key name without .NEW. added to the name
int get_offset_of_keyname(const char *idx_name)
{
    const char strnew[] = ".NEW.";
    int offset = 0;
    if (strncmp(strnew, idx_name, sizeof(strnew) - 1) == 0) {
        offset = sizeof(strnew) - 1;
    }
    return offset;
}

int sc_via_ddl_only()
{
    return bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_VIA_DDL_ONLY);
}

inline static int validate_ixname(const char *keynm)
{
    logmsg(LOGMSG_DEBUG, "Checking keynm '%s' \n", keynm);
    if (keynm == NULL) {
        logmsg(LOGMSG_ERROR, "Key name is NULL \n");
        return SC_BAD_INDEX_NAME;
    }
    const char *cptr = keynm;
    for (; *cptr != '\0' && *cptr != ' '; cptr++) {
        // go to ' ' or end of keynm
    }

    if (*cptr == ' ') {
        logmsg(LOGMSG_ERROR, "Key '%s' contains space character\n", keynm);
        return SC_BAD_INDEX_NAME;
    }
    if (cptr - keynm < 1) {
        logmsg(LOGMSG_ERROR, "Length of key '%s' must be > 0\n", keynm);
        return SC_BAD_INDEX_NAME;
    }
    if (cptr - keynm >= MAXIDXNAMELEN) {
        logmsg(LOGMSG_ERROR, "Length of key '%s' exceeds %d characters\n",
               keynm, MAXIDXNAMELEN - 1);
        return SC_BAD_INDEX_NAME;
    }
    return 0;
}

int validate_ix_names(struct dbtable *db)
{
    int rc = 0;
    for (int i = 0; i < db->nix; ++i) {
        struct schema *index = db->ixschema[i];
        int offset = get_offset_of_keyname(index->csctag);
        const char *keynm = index->csctag + offset;
        rc = validate_ixname(keynm);
        if (rc) break;
    }
    return rc;
}
