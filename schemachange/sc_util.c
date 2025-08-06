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

#include "sc_util.h"

#include "cdb2_constants.h"
#include "logmsg.h"
#include "schemachange.h"
#include "str_util.h"

int close_all_dbs_tran(tran_type *tran)
{
    int ii, rc, bdberr;
    struct dbtable *db;
    logmsg(LOGMSG_DEBUG, "Closing all tables...\n");
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        rc = bdb_close_only_sc(db->handle, tran, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "failed closing table '%s': %d\n",
                   db->tablename, bdberr);
            return -1;
        }
    }
    logmsg(LOGMSG_DEBUG, "Closed all tables OK\n");
    return 0;
}

int close_all_dbs(void)
{
    return close_all_dbs_tran(NULL);
}

int open_all_dbs_tran(void *tran)
{
    int ii, rc, bdberr;
    struct dbtable *db;
    logmsg(LOGMSG_DEBUG, "Opening all tables\n");
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        rc = bdb_open_again_tran(db->handle, tran, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR,
                   "morestripe: failed reopening table '%s': %d\n",
                   db->tablename, bdberr);
            return -1;
        }
    }
    logmsg(LOGMSG_DEBUG, "Opened all tables OK\n");
    gbl_sc_commit_count++;
    return 0;
}

int open_all_dbs(void)
{
    return open_all_dbs_tran(NULL);
}

/* Check if it is, in prinicple, ok to start a schema change. */
int check_sc_ok(struct schema_change_type *s)
{
    /* I must be rtcpu up */
    if (!is_node_up(gbl_myhostname)) {
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
               __FILE__, __LINE__, rc, *bdberr);
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
    sprintf(tmpname, "%s%s", prefix, db->tablename);

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

static inline int validate_ixname(const char *keynm, struct ireq *iq)
{
    logmsg(LOGMSG_DEBUG, "Checking keynm '%s' \n", keynm);
    if (keynm == NULL) {
        sc_client_error(iq->sc, "Key name cannot be NULL");
        return SC_BAD_INDEX_NAME;
    }
    const char *cptr = keynm;
    for (; *cptr != '\0' && *cptr != ' '; cptr++) {
        // go to ' ' or end of keynm
    }

    if (*cptr == ' ') {
        sc_client_error(iq->sc, "Key '%s' cannot contain spaces", keynm);
        return SC_BAD_INDEX_NAME;
    }
    if (cptr - keynm < 1) {
        sc_client_error(iq->sc, "Key name cannot be empty");
        return SC_BAD_INDEX_NAME;
    }
    if (cptr - keynm >= MAXIDXNAMELEN) {
        sc_client_error(iq->sc, "Length of key '%s' cannot exceed %d characters", keynm, MAXIDXNAMELEN - 1);
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
        rc = validate_ixname(keynm, db->iq);
        if (rc) break;
    }
    return rc;
}

int validate_table_name(const char *const name, size_t len,
                        int check_for_illegal_chars, const char **error) {
  if (len == 0) {
    if (error) {
      *error = comdb2_asprintf(
          "Table name cannot be empty. length=%zu name='%s'", len, name);
    }
    return 1;
  }

  if (len > MAXTABLELEN - 1) {
    if (error) {
      *error = comdb2_asprintf(
          "Table name is too long. max_length=%d length=%zu name='%s'",
          MAXTABLELEN - 1, len, name);
    }
    return 2;
  }

  if (check_for_illegal_chars &&
      !str_is_alphanumeric(name, NON_ALPHANUM_CHARS_ALLOWED_IN_TABLENAME)) {
    if (error) {
      *error = comdb2_asprintf(
          "Table name contains illegal characters. name='%s'", name);
    }
    return 3;
  }

  return 0;
}

static int seed_qsort_cmpfunc(const void *key1, const void *key2)
{
    sc_hist_row *s1, *s2;
    s1 = (sc_hist_row *)key1;
    s2 = (sc_hist_row *)key2;

    return bdb_cmp_genids(s1->seed, s2->seed);
}

// keep only last N sc history entries
int trim_sc_history_entries(tran_type *tran, const char *tablename)
{
    int rc = 0, bdberr, nkeys;
    sc_hist_row *hist = NULL;

    rc = bdb_llmeta_get_sc_history(tran, &hist, &nkeys, &bdberr, tablename);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to get all schema change hist\n",
               __func__);
        return 1;
    }
    int attr = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_HIST_KEEP);
    if (nkeys < attr)
        goto cleanup; // nothing to trim

#if defined(_SUN_SOURCE)
    qsort(hist, nkeys, sizeof(sc_hist_row), seed_qsort_cmpfunc);
#endif

    for (int i = 0; i < nkeys - attr; i++) {
        logmsg(LOGMSG_DEBUG, "Deleting sc_hist entry %i seed %0#16" PRIx64 "\n",
               i, hist[i].seed);

        rc = bdb_del_schema_change_history(tran, tablename, hist[i].seed);
        if (rc)
            return rc;
    }

cleanup:
    free(hist);
    return rc;
}
