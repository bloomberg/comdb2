/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include "limit_fortify.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "schemachange.h"
#include "sc_csc2.h"
#include "debug_switches.h"
#include "machine.h"
#include "logmsg.h"

int load_db_from_schema(struct schema_change_type *s, struct dbenv *thedb,
                        int *foundix, struct ireq *iq)
{
    /* load schema from string or file */
    int rc = dyns_load_schema_string(s->newcsc2, thedb->envname, s->table);
    if (rc != 0) {
        char *err;
        char *syntax_err;
        err = csc2_get_errors();
        syntax_err = csc2_get_syntax_errors();
        if (iq) reqerrstr(iq, ERR_SC, "%s", syntax_err);
        sc_errf(s, "%s", err);
        sc_errf(s, "Failed to load schema\n");
        return SC_INTERNAL_ERROR;
    }

    /* find which db has a matching name */
    if ((*foundix = getdbidxbyname(s->table)) < 0) {
        logmsg(LOGMSG_FATAL, "couldnt find table <%s>\n", s->table);
        exit(1);
    }

    return SC_OK;
}

/* Given a table name, this makes sure that what we know about the schema
 * matches what is written in our meta table.  If we have nothing in our table
 * we populate it from the given file. */
int check_table_schema(struct dbenv *dbenv, const char *table,
                       const char *csc2file)
{
    int version;
    char *meta_csc2 = NULL;
    int meta_csc2_len;
    char *file_csc2 = NULL;
    int rc;
    struct dbtable *db;

    if (debug_switch_skip_table_schema_check()) return 0;

    db = get_dbtable_by_name(table);
    if (!db) {
        logmsg(LOGMSG_ERROR, "check_table_schema: no such table %s\n", table);
        return -1;
    }

    version = get_csc2_version(table);
    if (version < 0) {
        logmsg(LOGMSG_ERROR,
               "check_table_schema: error getting current schema version\n");
        return -1;
    }

    meta_csc2 = NULL;
    rc = get_csc2_file(table, version, &meta_csc2, &meta_csc2_len);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "check_table_schema: could not load meta schema %d for table %s "
               "rcode %d\n",
               version, table, rc);
        if (version > 0) {
            /* if we have versions then we expect the schema to have been
             * stored,
             * complain bitterly if it isn't */
            return -1;
        }
    }

    if (meta_csc2) {
        /* see if the loaded schema differs from the schema contained
         * in our meta table. */
        if (schema_cmp(dbenv, db, meta_csc2) != 0) {
            logmsg(LOGMSG_ERROR, "SCHEMA MIS-MATCH FOR TABLE %s.\n", table);
            logmsg(LOGMSG_ERROR, "THIS IS MY SCHEMA (VERSION %d):-\n", version);
            logmsg(LOGMSG_ERROR, "%s\n", meta_csc2);
            rc = -1;
        }
    } else if (dbenv->master == machine()) {
        /* on master node, store schema if we don't have one already. */
        file_csc2 = load_text_file(csc2file);
        if (!file_csc2) {
            if (meta_csc2) free(meta_csc2);
            return -1;
        }

        logmsg(LOGMSG_ERROR,
               "check_table_schema: no schema loaded, loading now\n");
        rc = load_new_table_schema(dbenv, table, file_csc2);
    } else {
        /* in a cluster maybe we just can't load our schema.  accept this
         * and try again another time. */
        logmsg(LOGMSG_ERROR,
               "check_table_schema: not master, cannot load schema\n");
        rc = 0;
    }

    if (meta_csc2) free(meta_csc2);
    if (file_csc2) free(file_csc2);
    return rc;
}

int schema_cmp(struct dbenv *dbenv, struct dbtable *db, const char *csc2cmp)
{
    int rc;

    rc = dyns_load_schema_string((char *)csc2cmp, dbenv->envname, db->dbname);
    if (rc) {
        logmsg(LOGMSG_ERROR, "schema_cmp: error loading comparison schema\n");
        return -1;
    }

    /* create the .NEW. tags, but don't update db->lrl, numix, numblobs
     * or anything like that. */
    rc = add_cmacc_stmt_no_side_effects(db, 1);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "schema_cmp: error creating schema from comparison text\n");
        return -1;
    }

    rc = compare_all_tags(db->dbname, stderr);

    backout_schemas(db->dbname);

    return rc;
}

/* adds a new version of a schema (loaded from a file) to the meta table */
int load_new_table_schema_file_tran(struct dbenv *dbenv, tran_type *tran,
                                    const char *table, const char *csc2file)
{
    char *text;

    text = load_text_file(csc2file);

    if (text) {
        int rc;
        rc = load_new_table_schema_tran(dbenv, tran, table, text);
        free(text);
        return rc;
    } else
        return -1;
}

/* call load_new_table_schema_tran without a transaction */
int load_new_table_schema_file(struct dbenv *dbenv, const char *table,
                               const char *csc2file)
{
    return load_new_table_schema_file_tran(dbenv, NULL, table, csc2file);
}

/* adds a new version of a schema to the meta table */
int load_new_table_schema_tran(struct dbenv *dbenv, tran_type *tran,
                               const char *table, const char *csc2_text)
{
    int rc;
    int version;
    struct dbtable *db = get_dbtable_by_name(table);

    if (debug_switch_skip_table_schema_check()) return 0;
    if (db && db->sc_to) {
        version = db->sc_to->version;
    } else {
        version = get_csc2_version_tran(table, tran);
        if (version < 0 || db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: error getting schema\n", __func__);
            return -1;
        }

        /* If instant schema change is enabled
         * then, we don't increment version if
         * there was no change to schema.
         *
         * If we are creating tables, then there
         * is no llmeta version record. */
        if (gbl_create_mode || !db->instant_schema_change) {
            ++version;
        }
    }

    rc = put_csc2_file(table, tran, version, csc2_text);
    if (rc != 0) {
        logmsg(
            LOGMSG_ERROR,
            "load_new_table_schema: unable to load table %s csc2 version %d\n",
            table, version);
        return -1;
    }

    return 0;
}

/* calls load_new_table_schema without a trasaction */
int load_new_table_schema(struct dbenv *dbenv, const char *table,
                          const char *csc2_text)
{
    return load_new_table_schema_tran(dbenv, NULL, table, csc2_text);
}
int write_csc2_file_fname(const char *fname, const char *csc2text)
{
    char fnamesav[256];
    int fd;
    int len = strlen(csc2text);
    int nbytes;

    /* Make a .sav of the file. */
    snprintf(fnamesav, sizeof(fnamesav), "%s.sav", fname);
    if (rename(fname, fnamesav) == -1) {
        if (errno != ENOENT) {
            logmsg(LOGMSG_ERROR,
                   "write_csc2_file_fname: error renaming %s -> %s: %d %s\n",
                   fname, fnamesav, errno, strerror(errno));
        }
    }

    /* Dump new csc2 text into file. */
    fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd == -1) {
        logmsg(LOGMSG_ERROR,
               "write_csc2_file_fname: error opening %s for writing: %d %s\n",
               fname, errno, strerror(errno));
        return -1;
    }
    while (len > 0) {
        nbytes = write(fd, csc2text, len);
        if (nbytes < 0) {
            logmsg(LOGMSG_ERROR,
                   "write_csc2_file_fname: error writing to %s: %d %s\n", fname,
                   errno, strerror(errno));
            close(fd);
            return -1;
        }
        len -= nbytes;
        csc2text += nbytes;
    }
    if (close(fd) == -1) {
        logmsg(LOGMSG_ERROR, "write_csc2_file_fname: error closing %s: %d %s\n",
               fname, errno, strerror(errno));
    }

    logmsg(LOGMSG_DEBUG, "wrote csc2 file '%s'\n", fname);

    return 0;
}

int write_csc2_file(struct dbtable *db, const char *csc2text)
{
    char fnamedefault[256];

    if (get_generic_csc2_fname(db, fnamedefault, sizeof(fnamedefault))) {
        logmsg(LOGMSG_ERROR, "Failed to create fname\n");
        return -1;
    }

    return write_csc2_file_fname(fnamedefault, csc2text);
}

int get_csc2_fname(const struct dbtable *db, const char *dir, char *fname,
                   size_t fname_len)
{
    int rc;

    rc = snprintf(fname, fname_len, "%s/%s.csc2", dir, db->dbname);
    if (rc < 0 || rc >= fname_len) return -1;

    return 0;
}

int get_generic_csc2_fname(const struct dbtable *db, char *fname, size_t fname_len)
{
    return get_csc2_fname(db, thedb->basedir, (char *)fname, fname_len);
}

/* write out all the schemas from meta for all open tables to disk */
int dump_all_csc2_to_disk()
{
    int ii;
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        if (thedb->dbs[ii]->dbtype == DBTYPE_TAGGED_TABLE) {
            int rc;
            int version;
            char *meta_csc2 = NULL;
            int meta_csc2_len;

            version = get_csc2_version(thedb->dbs[ii]->dbname);
            if (version < 0) {
                logmsg(LOGMSG_ERROR,
                       "dump_all_csc2_to_disk: error getting current schema "
                       "version for table %s\n",
                       thedb->dbs[ii]->dbname);
                return -1;
            }

            rc = get_csc2_file(thedb->dbs[ii]->dbname, version, &meta_csc2,
                               &meta_csc2_len);
            if (rc != 0 || !meta_csc2) {
                logmsg(LOGMSG_ERROR,
                       "dump_all_csc2_to_disk: could not load meta schema %d "
                       "for table %s rcode %d\n",
                       version, thedb->dbs[ii]->dbname, rc);
                return -1;
            }

            rc = write_csc2_file(thedb->dbs[ii], meta_csc2);
            free(meta_csc2);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR,
                       "error printing out schema for table '%s'\n",
                       thedb->dbs[ii]->dbname);
                return -1;
            }
        }
    }

    return 0; /*success*/
}

/* TODO clean up all these csc2 dump functions, unify them */
/* write out all the schemas from meta for all open tables to disk */
int dump_table_csc2_to_disk_fname(struct dbtable *db, const char *csc2_fname)
{
    int rc;
    int version;
    char *meta_csc2 = NULL;
    int meta_csc2_len;

    version = get_csc2_version(db->dbname);
    if (version < 0) {
        logmsg(LOGMSG_ERROR, "%s: error getting current schema "
                             "version for table %s\n",
               __func__, db->dbname);
        return -1;
    }

    rc = get_csc2_file(db->dbname, version, &meta_csc2, &meta_csc2_len);
    if (rc != 0 || !meta_csc2) {
        logmsg(LOGMSG_ERROR, "%s: could not load meta schema %d "
                             "for table %s rcode %d\n",
               __func__, version, db->dbname, rc);
        return -1;
    }

    rc = write_csc2_file_fname(csc2_fname, meta_csc2);
    free(meta_csc2);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "error printing out schema for table '%s'\n",
               db->dbname);
        return -1;
    }

    logmsg(LOGMSG_DEBUG, "rewrote csc2 file for table %s\n", db->dbname);

    return 0;
}

/* write out all the schemas from meta for all open tables to disk */
int dump_table_csc2_to_disk(const char *table)
{
    struct dbtable *p_db;
    int rc;
    int version;
    char *meta_csc2 = NULL;
    int meta_csc2_len;

    if (!(p_db = get_dbtable_by_name(table))) return 1;

    if (p_db->dbtype != DBTYPE_TAGGED_TABLE) return 1;

    if ((version = get_csc2_version(p_db->dbname)) < 0) {
        logmsg(LOGMSG_ERROR,
               "dump_all_csc2_to_disk: error getting current schema version "
               "for table %s\n",
               p_db->dbname);
        return -1;
    }

    if ((rc = get_csc2_file(p_db->dbname, version, &meta_csc2,
                            &meta_csc2_len)) ||
        !meta_csc2) {
        logmsg(LOGMSG_ERROR,
               "dump_all_csc2_to_disk: could not load meta schema %d for table "
               "%s rcode %d\n",
               version, p_db->dbname, rc);
        return -1;
    }

    rc = write_csc2_file(p_db, meta_csc2);
    free(meta_csc2);
    if (rc) {
        logmsg(LOGMSG_ERROR, "error printing out schema for table '%s'\n",
               p_db->dbname);
        return -1;
    }

    logmsg(LOGMSG_DEBUG, "rewrote csc2 file for table %s\n", p_db->dbname);

    return 0;
}
