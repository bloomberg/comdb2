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
    struct db *db;
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
    struct db *db;
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
char *get_temp_db_name(struct db *db, char *prefix, char tmpname[])
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

int validate_ix_names(struct db *db)
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

int init_history_sc(struct schema_change_type *s, struct db *db,
                    struct schema_change_type *scopy)
{
    scopy->sb = s->sb;
    scopy->must_close_sb = 0;
    scopy->nothrevent = 1;
    scopy->live = 1;
    scopy->type = DBTYPE_TAGGED_TABLE;
    scopy->finalize = 0;
    if (strlen(db->dbname) + strlen("_history") + 1 > MAXTABLELEN) {
        sc_errf(s, "History table name too long\n");
        free_schema_change_type(scopy);
        return SC_CSC2_ERROR;
    }
    snprintf(scopy->table, sizeof(scopy->table), "%s_history", db->dbname);
    scopy->table[sizeof(scopy->table) - 1] = '\0';
    if (scopy->newcsc2) free(scopy->newcsc2);
    scopy->newcsc2 = NULL;
    scopy->headers = s->headers;
    scopy->compress = s->compress;
    scopy->compress_blobs = s->compress_blobs;
    scopy->ip_updates = s->ip_updates;
    scopy->instant_sc = s->instant_sc;
    scopy->force_rebuild = s->force_rebuild;
    scopy->force_dta_rebuild = s->force_dta_rebuild;
    scopy->force_blob_rebuild = s->force_blob_rebuild;
    scopy->is_history = 1;
    scopy->orig_db = db;
    s->history_s = scopy;

    return 0;
}

char *sql_field_default_trans(struct field *f, int is_out);
char *generate_history_csc2(struct db *db)
{
    struct schema *schema;
    int field;
    strbuf *csc2;
    int ixnum;
    char buf[128];
    char *outcsc2 = NULL;

    schema = db->schema;
    csc2 = strbuf_new();
    strbuf_clear(csc2);

    strbuf_append(csc2, "\n");
    strbuf_append(csc2, "tag ondisk\n");
    strbuf_append(csc2, "{\n");
    for (field = 0; field < schema->nmembers; field++) {
        strbuf_append(csc2, "\t");
        strbuf_append(csc2, csc2type(&schema->member[field]));
        strbuf_append(csc2, "\t");
        strbuf_append(csc2, schema->member[field].name);
        switch (schema->member[field].type) {
        case SERVER_BYTEARRAY:
            strbuf_append(csc2, "[");
            snprintf(buf, 128, "%d", schema->member[field].len - 1);
            strbuf_append(csc2, buf);
            strbuf_append(csc2, "]");
            break;
        case SERVER_BCSTR:
        case CLIENT_CSTR:
        case CLIENT_PSTR2:
        case CLIENT_PSTR:
        case CLIENT_BYTEARRAY:
            strbuf_append(csc2, "[");
            snprintf(buf, 128, "%d", schema->member[field].len);
            strbuf_append(csc2, buf);
            strbuf_append(csc2, "]");
            break;
        case CLIENT_VUTF8:
        case SERVER_VUTF8:
            if (schema->member[field].len > 5) {
                strbuf_append(csc2, "[");
                snprintf(buf, 128, "%d", schema->member[field].len - 5);
                strbuf_append(csc2, buf);
                strbuf_append(csc2, "]");
            }
            break;
        default: break;
        }
        if (!(schema->member[field].flags & NO_NULL))
            strbuf_append(csc2, " null=yes");
        if (schema->member[field].in_default) {
            strbuf_append(csc2, " dbstore=");
            strbuf_append(csc2,
                          sql_field_default_trans(&(schema->member[field]), 0));
        }
        if (schema->member[field].out_default) {
            strbuf_append(csc2, " dbload=");
            strbuf_append(csc2,
                          sql_field_default_trans(&(schema->member[field]), 1));
        }
        strbuf_append(csc2, "\n");
    }
    strbuf_append(csc2, "}\n");
    if (db->nix > 0) {
        strbuf_append(csc2, "keys\n");
        strbuf_append(csc2, "{\n");
        /* do the indices */
        for (ixnum = 0; ixnum < db->nix; ixnum++) {
            schema = db->ixschema[ixnum];
            strbuf_append(csc2, "\t dup ");
            if (schema->flags & SCHEMA_DATACOPY) {
                strbuf_append(csc2, "datacopy ");
            }
            if (schema->flags & SCHEMA_RECNUM) {
                strbuf_append(csc2, "recnum ");
            }
            strbuf_append(csc2, "\"");
            if (strncasecmp(schema->csctag, ".NEW.", 5) == 0)
                strbuf_append(csc2, schema->csctag + 5);
            else
                strbuf_append(csc2, schema->csctag);
            strbuf_append(csc2, "\" = ");
            for (field = 0; field < schema->nmembers; field++) {
                if (field > 0) strbuf_append(csc2, " + ");
                if (schema->member[field].flags & INDEX_DESCEND)
                    strbuf_append(csc2, "<DESCEND> ");
                if (schema->member[field].isExpr) {
                    strbuf_append(csc2, "(");
                    strbuf_append(csc2, csc2type(&schema->member[field]));
                    switch (schema->member[field].type) {
                    case SERVER_BYTEARRAY:
                        strbuf_append(csc2, "[");
                        snprintf(buf, 128, "%d", schema->member[field].len - 1);
                        strbuf_append(csc2, buf);
                        strbuf_append(csc2, "]");
                        break;
                    case SERVER_BCSTR:
                    case CLIENT_CSTR:
                    case CLIENT_PSTR2:
                    case CLIENT_PSTR:
                    case CLIENT_BYTEARRAY:
                        strbuf_append(csc2, "[");
                        snprintf(buf, 128, "%d", schema->member[field].len);
                        strbuf_append(csc2, buf);
                        strbuf_append(csc2, "]");
                        break;
                    case CLIENT_VUTF8:
                    case SERVER_VUTF8:
                        if (schema->member[field].len > 5) {
                            strbuf_append(csc2, "[");
                            snprintf(buf, 128, "%d",
                                     schema->member[field].len - 5);
                            strbuf_append(csc2, buf);
                            strbuf_append(csc2, "]");
                        }
                        break;
                    default: break;
                    }
                    strbuf_append(csc2, ")\"");
                }
                strbuf_append(csc2, schema->member[field].name);
                if (schema->member[field].isExpr) {
                    strbuf_append(csc2, "\"");
                }
            }
            if (schema->where) {
                strbuf_append(csc2, " {");
                strbuf_append(csc2, schema->where);
                strbuf_append(csc2, "}");
            }
            strbuf_append(csc2, "\n");
        }
        strbuf_append(csc2, "}\n");
    }
    outcsc2 = strdup(strbuf_buf(csc2));
    strbuf_free(csc2);
    return outcsc2;
}
