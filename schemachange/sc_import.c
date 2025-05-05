/**
 * Import allows you to quickly copy a table from one db to another.
 * This is useful when you have to replace a table's
 * contents (i.e. you get a new batch of information every month) but you don't
 * want to slow the main db down by adding all the new records and deleting the
 * old ones in a batch update.
 *
 * This is how it works:
 *
 * 1. Clients issue the SQL statement "import <src_table_name> from <src_db_name> to <dst_table_name>"
 * on the destination db.
 *
 * 2. The destination db starts a comdb2 database in "import mode" that lives in 
 * the destination db's tmp directory (eg: dbdir/tmp/import)
 *
 * 3. The import database grabs all files needed for recovery from the source db
 * by selecting from comdb2_files and then writes these files into its directory.
 *
 * 4. The import database runs full recovery using the files it grabbed from the source.
 *
 * 5. The import database writes information about the import target table 
 * to a file and then terminates.
 *
 * 6. The destination db reads the file written by the import database and uses this 
 * information to copy the btree files associated with the target table from the 
 * import database directory into its directory, copy the files to the replicants,
 * and finally to overwrite the existing table with a schema change.
 */

#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pb_alloc.h>
#include <unistd.h>

#include "fdb_fend.h"
#include "bdb_api.h"
#include "bdb_schemachange.h"
#include "schemachange.h"
#include "comdb2.h"
#include "comdb2_appsock.h"
#include "crc32c.h"
#include "importdata.pb-c.h"
#include "intern_strings.h"
#include "logmsg.h"
#include "sc_callbacks.h"
#include "sc_global.h"
#include "str0.h"
#include "version_util.h"
#include "str_util.h"
#include "sc_import.h"

#define BULK_IMPORT_MIN_SUPPORTED_VERSION "8.1.0"

#define __import_logmsg(lvl, msg, ...) \
    logmsg(lvl, "[IMPORT] %s: " msg, __func__, ## __VA_ARGS__)

static int value_on_off(const char *value) {
    if (strcasecmp("on", value) == 0) {
        return 1;
    } else if (strcasecmp("off", value) == 0) {
        return 0;
    } else if (strcasecmp("no", value) == 0) {
        return 0;
    } else if (strcasecmp("yes", value) == 0) {
        return 1;
    }
    return atoi(value);
}

extern char *comdb2_get_tmp_dir();
extern int comdb2_get_tmp_dir_args(char *tmp_dir, ssize_t sz, const char *basedir, const char *envname, int nonames);
extern int get_txndir_args(char *txndir, size_t sz_txndir, const char *dbdir, const char *envname, int nonames);

extern tran_type *curtran_gettran(void);
extern void curtran_puttran(tran_type *tran);

int gbl_debug_sleep_during_bulk_import = 0;
extern int gbl_import_mode;
extern char *gbl_import_table;
extern char *gbl_file_copier;
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern char *gbl_dbdir;

extern int should_ignore_btree(const char *filename,
                               int (*should_ignore_table)(const char *),
                               int should_ignore_queues,
                               int name_boundary_exists);

static uint64_t gbl_import_id = 0;
pthread_mutex_t import_id_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Constants */
#define FILENAMELEN 100

int get_str_ending_from_list(const char *str, const char **list) {
    const char * ending;
    for (int i=0; ((ending = list[i]), *ending != '\0'); ++i) {
        if (str_has_ending(str, ending)) {
            return i;
        }
    }
    return -1;
}

static int is_named_table(const char *tablename) {
    static const char * named_table_endings[] =
        {".metadata", ".llmeta", "" /* delimiter */};

    return get_str_ending_from_list(tablename, named_table_endings) >= 0;
}

int bulk_import_tmpdb_should_ignore_table(const char *table) {
    const char * required_tables[] = {
        "comdb2_llmeta",
        "comdb2_metadata",
        "sqlite_stat1",
        "sqlite_stat4",
        gbl_import_table,
        "" /* sentinel */ };

    if (is_named_table(table)) {
        return 0;
    }

    const char * required_table;
    for (int i=0; (required_table = required_tables[i]), required_table[0] != '\0'; ++i) {
        if (strcmp(required_table, table) == 0) {
            return 0;
        }
    }

    return 1;
}

int bulk_import_tmpdb_should_ignore_btree(const char *filename)
{
    return should_ignore_btree(filename, bulk_import_tmpdb_should_ignore_table, 0, 1);
}

static void get_import_dbdir(char *import_dbdir, size_t sz_import_dbdir, const uint64_t import_id) {
    snprintf(import_dbdir, sz_import_dbdir, "%s/import%"PRIu64"", comdb2_get_tmp_dir(), import_id);
}

static int bulk_import_get_import_data_fname(char *import_data_fname,
                                              size_t sz_import_data_fname, const uint64_t import_id) {
    int rc;
    char import_dbdir[PATH_MAX];

    rc = 0;

    if (!gbl_import_mode) {
        get_import_dbdir(import_dbdir, sizeof(import_dbdir), import_id);
    } else {
        strncpy(import_dbdir, thedb->basedir, sizeof(import_dbdir));
    }

    rc = snprintf(import_data_fname, sz_import_data_fname, "%s/%s", import_dbdir,
             "bulk_import_data") < 0;

    return rc;
}

/*
 * Copies a file to a node using the tool given by the
 * `file_copier` tunable.
 */
static enum comdb2_import_op bulk_import_copy_file_to_replicant(const char *dst_path,
                                       const char *hostname) {
    int rc = COMDB2_IMPORT_RC_SUCCESS;
    char *command = NULL;

    const int offset = snprintf(NULL, 0, "%s -r %s %s:%s", gbl_file_copier, dst_path,
                      hostname, dst_path);
    command = malloc(offset+1);
    if (!command) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    sprintf(command, "%s -r %s %s:%s", gbl_file_copier, dst_path,
                     hostname, dst_path);

    rc = system(command);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to copy %s to replicant %s\n",
               dst_path, hostname);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

err:
    if (command) {
        free(command);
    }
    return rc;
}

/*
 * Cleans up all resources created in `bulk_import_setup_import_db`.
 */
static enum comdb2_import_op bulk_import_cleanup_import_db(char *tmp_db_dir) {
    int rc = COMDB2_IMPORT_RC_SUCCESS;

    const int size = snprintf(NULL, 0, "rm -rf %s", tmp_db_dir) + 1;
    char * command = malloc(size);
    if (!command) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    sprintf(command, "rm -rf %s", tmp_db_dir);
    if ((rc = system(command)), rc != 0) {
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    free(tmp_db_dir);

err:
    if (command) {
        free(command);
    }

    return rc;
}

// Called on startup to clean any existing import dirs
// (there can be leftover import dirs on startup if there's 
// a crash during an import)
int bulk_import_cleanup_import_dirs() {
    int rc = 0;
    const char * const tmp_dir = comdb2_get_tmp_dir();

    const int size = snprintf(NULL, 0, "rm -rf %s/import*", tmp_dir) + 1;
    char * command = malloc(size);
    if (!command) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = -1;
        goto err;
    }

    sprintf(command, "rm -rf %s/import*", tmp_dir);
    if ((rc = system(command)), rc != 0) {
        __import_logmsg(LOGMSG_ERROR, "Command '%s' failed\n", command);
        rc = -1;
        goto err;
    }

err:
    free(command);
    return rc;
}

static enum comdb2_import_op bulk_import_write_import_db_lrl(char *tmp_db_dir, const char *srcdb) {
    FILE *fp = NULL;
    int rc = COMDB2_IMPORT_RC_SUCCESS;

    char fname[PATH_MAX];
    int t_rc = snprintf(fname, sizeof(fname), "%s/import.lrl", tmp_db_dir);
    if (t_rc < 0 || t_rc >= sizeof(fname)) {
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    fp = fopen(fname, "w");
    if (!fp) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to open %s for writing with errno %s\n",
               fname, strerror(errno));
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    fprintf(fp, "name import\ndir %s\nnonames\ndtastripe %d\n%s",
            tmp_db_dir,
            gbl_dtastripe,
            gbl_blobstripe ? "blobstripe" : "noblobstripe");

err:
    if (fp) {
        t_rc = fclose(fp) ? COMDB2_IMPORT_RC_INTERNAL : 0;
        rc = rc ? rc : t_rc;;
    }

    return rc;
}

/*
 * Creates all directories and files needed to run the import db.
 */
static enum comdb2_import_op bulk_import_setup_import_db(char **p_tmp_db_dir, const uint64_t import_id, const char *srcdb) {
    int dbdir_created = 0;

    char tmp_db_dir[PATH_MAX];
    get_import_dbdir(tmp_db_dir, sizeof(tmp_db_dir), import_id);

    int rc = mkdir(tmp_db_dir, 0700);
    if (rc != 0) {
        // Don't exclude failure when the directory already exists:
        // The import directory is removed after the import; however it may
        // still exist if the remove failed. Running an import on an old
        // directory may result in undefined behavior.
        // TODO: Could probably just warn here because of archival.
        __import_logmsg(LOGMSG_ERROR,
               "Failed to create import dir '%s' with errno %s\n",
               tmp_db_dir, strerror(errno));
        goto err;
    }
    dbdir_created = 1;

    rc = bulk_import_write_import_db_lrl(tmp_db_dir, srcdb);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to write lrl file for import db\n");
        goto err;
    }

    *p_tmp_db_dir = strdup(tmp_db_dir);
    if (*p_tmp_db_dir == NULL) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        goto err;
    }

    return COMDB2_IMPORT_RC_SUCCESS;
err:
    if (dbdir_created) {
        char *dup_tmp_db_dir = strdup(tmp_db_dir);
        if (!dup_tmp_db_dir
            || bulk_import_cleanup_import_db(dup_tmp_db_dir)) {
            __import_logmsg(LOGMSG_ERROR,
                   "Failed to cleanup dir '%s'\n",
                   tmp_db_dir);
        }
    }
    return COMDB2_IMPORT_RC_INTERNAL;
}

/*
 * Loads import information generated by the import process.
 */
static enum comdb2_import_op bulk_import_data_unpack_from_file(ImportData **pp_data, const uint64_t import_id) {
    int rc = COMDB2_IMPORT_RC_SUCCESS;
    void *line = NULL;
    FILE *fp = NULL;

    char import_data_fname[PATH_MAX];
    bulk_import_get_import_data_fname(import_data_fname,
                                      sizeof(import_data_fname), import_id);

    fp = fopen(import_data_fname, "r");
    if (!fp) {
        __import_logmsg(LOGMSG_ERROR, "Failed to open file %s. err %s\n",
               import_data_fname, strerror(errno));
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    struct stat st;
    rc = stat(import_data_fname, &st);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to stat file %s. err %s\n",
               import_data_fname, strerror(errno));
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }
    const long fsize = st.st_size;

    line = malloc(fsize);
    if (!line) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    size_t num_read = fread(line, fsize, 1, fp);
    if (num_read < 1) {
        __import_logmsg(LOGMSG_ERROR,
               "Read less than expected from file %s. Num read = "
               "%lu and fsize = %lu\n",
               import_data_fname, num_read, fsize);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    *pp_data = import_data__unpack(&pb_alloc, fsize, line);
    if (*pp_data == NULL) {
        __import_logmsg(LOGMSG_ERROR, "Error unpacking incoming message\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

err:
    if (fp) {
        fclose(fp);
    }

    if (line) {
        free(line);
    }

    return rc;
}

void clear_bulk_import_data(ImportData *p_data) {
    if (p_data->index_genids) {
        free(p_data->index_genids);
    }
    if (p_data->blob_genids) {
        free(p_data->blob_genids);
    }
    if (p_data->table_name) {
        free(p_data->table_name);
    }
    if (p_data->data_dir) {
        free(p_data->data_dir);
    }
    for (int i = 0; i < p_data->n_data_files; ++i) {
        free(p_data->data_files[i]);
    }
    if (p_data->data_files) {
        free(p_data->data_files);
    }
    for (int i = 0; i < p_data->n_index_files; ++i) {
        free(p_data->index_files[i]);
    }
    if (p_data->index_files) {
        free(p_data->index_files);
    }
    for (int i = 0; i < p_data->n_csc2; ++i) {
        free(p_data->csc2[i]);
    }
    if (p_data->csc2) {
        free(p_data->csc2);
    }
    for (int i = 0; i < p_data->n_blob_files; ++i) {
        BlobFiles *b = p_data->blob_files[i];
        for (int j = 0; j < b->n_files; j++) {
            free(b->files[j]);
        }
        if (b->files) {
            free(b->files);
        }
        free(b);
    }
    if (p_data->blob_files) {
        free(p_data->blob_files);
    }

    *p_data = (const ImportData){ 0 };
}

/**
 * Grabs all the bulk import data for a local table
 */
static enum comdb2_import_op bulk_import_data_load(const char *table_name,
                                                   ImportData *p_data,
                                                   tran_type * const tran) {
    unsigned i, j;
    int bdberr;
    struct dbtable *db;
    char *p_csc2_text = NULL;
    char tempname[64 /*hah*/];
    tran_type *t = NULL;
    int len, pgsz;

    int rc = COMDB2_IMPORT_RC_SUCCESS;

    p_data->table_name = strdup(table_name);
    if (!p_data->table_name) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    /* clear data that may not get initialized*/
    p_data->compress = 0;
    p_data->compress_blobs = 0;

    /* find the table we're using */
    if (!(db = get_dbtable_by_name(table_name))) {
        __import_logmsg(LOGMSG_ERROR, "no such table: %s\n",
               table_name);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    /* get the data dir */
    p_data->data_dir = strdup(thedb->basedir);
    if (!p_data->data_dir) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    /* get table's schema from the meta table and calculate it's crc32 */
    if (get_csc2_file_tran(table_name, -1 /*highest csc2_version*/,
                      &p_csc2_text, NULL /*csc2len*/, tran)) {
        __import_logmsg(LOGMSG_ERROR,
               "could not get schema for table: %s\n",
               table_name);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    p_data->csc2_crc32 = crc32c((const void *)p_csc2_text, strlen(p_csc2_text));
    free(p_csc2_text);
    p_csc2_text = NULL;

    /* get checksums option */
    p_data->checksums = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CHECKSUMS);

    /* get odh options from the meta table */
    if ((get_db_odh_tran(db, &p_data->odh, tran) ||
        (p_data->odh && (get_db_compress_tran(db, &p_data->compress, tran) ||
                         get_db_compress_blobs_tran(db, &p_data->compress_blobs, tran))))
        && gbl_import_mode) {
        __import_logmsg(LOGMSG_ERROR,
               "failed to fetch odh flags for table: %s\n",
               table_name);
        rc = COMDB2_IMPORT_TMPDB_RC_NO_ODH_OPTION;
        goto err;
    }

    /* get stripe options */
    p_data->dtastripe = gbl_dtastripe;
    p_data->blobstripe = gbl_blobstripe;

    /* get data file's current version from meta table */
    if (bdb_get_file_version_data(db->handle, tran, 0 /*dtanum*/,
                                  (unsigned long long *)&p_data->data_genid,
                                  &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        __import_logmsg(LOGMSG_ERROR,
               "failed to fetch version number for %s's main data "
               "files\n", table_name);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    /* get page sizes from meta table */

    //  * Don't know if this is necessary.
    if (bdb_get_pagesize_data(db->handle, tran, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->data_pgsz = pgsz;
    } else {
        p_data->data_pgsz = -1;
    }
    if (bdb_get_pagesize_index(db->handle, tran, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->index_pgsz = pgsz;
    } else {
        p_data->index_pgsz = -1;
    }
    if (bdb_get_pagesize_blob(db->handle, tran, &pgsz, &bdberr) == 0 &&
        bdberr == 0) {
        p_data->blob_pgsz = pgsz;
    } else {
        p_data->blob_pgsz = -1;
    }

    /* get ipu/isc options from meta table */
    // Only an error if the source db doesn't have them

    if (get_db_inplace_updates_tran(db, &p_data->ipu, tran) && gbl_import_mode) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed to get inplace update option for table %s\n",
            table_name);
        rc = COMDB2_IMPORT_TMPDB_RC_NO_IPU_OPTION;
        goto err;
    }
    if (get_db_instant_schema_change_tran(db, &p_data->isc, tran) && gbl_import_mode) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to get instant schema change option for "
               "table %s\n",
               table_name);
        rc = COMDB2_IMPORT_TMPDB_RC_NO_ISC_OPTION;
        goto err;
    }
    if (get_db_datacopy_odh_tran(db, &p_data->dc_odh, tran) && gbl_import_mode) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to get datacopy odh option for table %s\n",
               table_name);
        rc = COMDB2_IMPORT_TMPDB_RC_NO_ODH_OPTION;
        goto err;
    }

    p_data->n_data_files = p_data->dtastripe;
    p_data->data_files = malloc(sizeof(char *) * p_data->n_data_files);
    if (!p_data->data_files) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    for (i = 0; i < p_data->dtastripe; i++) {
        len = bdb_form_file_name(db->handle, 1, 0, i, p_data->data_genid,
                                 tempname, sizeof(tempname));
        if (len <= 0 || len > 64) {
            __import_logmsg(LOGMSG_ERROR,
                   "failed to retrieve the data filename, stripe "
                   "%d\n", i);
        }
        p_data->data_files[i] = strdup(tempname);
        if (!p_data->data_files[i]) {
            __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
            rc = COMDB2_IMPORT_RC_INTERNAL;
            goto err;
        }
    }

    if (!tran) { t = curtran_gettran(); }
    int version = get_csc2_version_tran(table_name, tran ? tran : t);
    if (version == -1) {
        __import_logmsg(LOGMSG_ERROR,
               "Could not find csc2 version for table %s\n",
               table_name);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    p_data->n_csc2 = version;
    p_data->csc2 = malloc(sizeof(char *) * p_data->n_csc2);
    if (!p_data->csc2) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    for (int vers = 1; vers <= version; vers++) {
        get_csc2_file_tran(table_name, vers, &p_data->csc2[vers - 1],
                           &len, tran ? tran : t);
    }

    if (t) {
        curtran_puttran(t);
        t = NULL;
    }

    /* get num indicies/blobs */
    p_data->n_index_genids = db->nix;
    p_data->index_genids =
        malloc(sizeof(unsigned long int) * p_data->n_index_genids);
    if (!p_data->index_genids) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    p_data->n_index_files = p_data->n_index_genids;
    p_data->index_files = malloc(sizeof(char *) * p_data->n_index_files);
    if (!p_data->index_files) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    p_data->n_blob_genids = db->numblobs;
    p_data->blob_genids =
        malloc(sizeof(unsigned long int) * p_data->n_blob_genids);
    if (!p_data->blob_genids) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    /* for each index, lookup version */
    for (i = 0; i < p_data->n_index_genids; ++i) {
        /* get index file's current version from meta table */
        if (bdb_get_file_version_index(
                db->handle, tran, i /*ixnum*/,
                (unsigned long long *)&p_data->index_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            __import_logmsg(LOGMSG_ERROR,
                   "failed to fetch version number for %s's "
                   "index: %d files\n",
                   table_name, i);
            rc = COMDB2_IMPORT_RC_INTERNAL;
            goto err;
        }

        len = bdb_form_file_name(db->handle, 0, 0, i, p_data->index_genids[i],
                                 tempname, sizeof(tempname));
        if (len <= 0 || len > 64) {
            __import_logmsg(
                LOGMSG_ERROR,
                "failed to retrieve the index filename, ix %d\n",
                i);
        }
        p_data->index_files[i] = strdup(tempname);
    }

    if (p_data->n_blob_genids > 0) {
        p_data->n_blob_files = p_data->n_blob_genids;
        p_data->blob_files = malloc(sizeof(BlobFiles *) * p_data->n_blob_files);
        if (!p_data->blob_files) {
            __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
            rc = COMDB2_IMPORT_RC_INTERNAL;
            goto err;
        }

        for (int i = 0; i < p_data->n_blob_files; ++i) {
            p_data->blob_files[i] = malloc(sizeof(BlobFiles));
            if (!p_data->blob_files[i]) {
                __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
                rc = COMDB2_IMPORT_RC_INTERNAL;
                goto err;
            }

            BlobFiles *b = p_data->blob_files[i];
            *b = (BlobFiles) BLOB_FILES__INIT;
            b->n_files = p_data->blobstripe ? p_data->dtastripe : 1;
            b->files = malloc(sizeof(char *) * b->n_files);
            if (!b->files) {
                __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
                rc = COMDB2_IMPORT_RC_INTERNAL;
                goto err;
            }
        }
    }

    /* for each blob, lookup and compare versions */
    for (i = 0; i < p_data->n_blob_genids; ++i) {
        /* get blob file's current version from meta table */
        if (bdb_get_file_version_data(
                db->handle, tran, i + 1 /*dtanum*/,
                (unsigned long long *)&p_data->blob_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            __import_logmsg(LOGMSG_ERROR,
                   "failed to fetch version number for %s's "
                   "blob: %d files\n",
                   table_name, i);
            rc = COMDB2_IMPORT_RC_INTERNAL;
            goto err;
        }

        if (p_data->blobstripe) {
            for (j = 0; j < p_data->dtastripe; j++) {
                len = bdb_form_file_name(db->handle, 1, i + 1, j,
                                         p_data->blob_genids[i], tempname,
                                         sizeof(tempname));
                if (len <= 0 || len > 64) {
                    __import_logmsg(LOGMSG_ERROR,
                           "failed to retrieve the blob "
                           "filename, ix %d stripe %d\n",
                           i, j);
                }
                p_data->blob_files[i]->files[j] = strdup(tempname);
            }
        } else {
            len = bdb_form_file_name(db->handle, 1, i + 1, 0,
                                     p_data->blob_genids[i], tempname,
                                     sizeof(tempname));
            if (len <= 0 || len > 64) {
                __import_logmsg(LOGMSG_ERROR,
                       "failed to retrieve the blob filename, "
                       "ix %d stripe %d\n",
                       i, 0);
            }
            p_data->blob_files[i]->files[0] = strdup(tempname);
        }
    }

err:
    if (t) {
        curtran_puttran(t);
    }

    if (rc) {
        clear_bulk_import_data(p_data);
    }

    return rc;
}

/**
 * Performs all the checks necessary to make sure that we are capable of doing a
 * bulk import.
 */
static enum comdb2_import_op 
bulk_import_data_validate(const char *dst_table_name,
                          const int src_dtastripe,
                          const int src_blobstripe,
                          tran_type * const tran) {
    if (thedb->master != gbl_myhostname) {
        __import_logmsg(LOGMSG_ERROR, "I'm not the master\n");
        return COMDB2_IMPORT_RC_INTERNAL;
    }

    struct dbtable * const db = get_dbtable_by_name(dst_table_name);
    if (!db) {
        __import_logmsg(LOGMSG_ERROR,
                "Destination table '%s' was not found\n", dst_table_name);
        return COMDB2_IMPORT_RC_NO_DST_TBL;
    }

    unsigned long long genid;
    if (get_blobstripe_genid_tran(db, &genid, tran) == 0) {
        __import_logmsg(LOGMSG_ERROR,
                "Destination db has a blobstripe genid\n");
        return COMDB2_IMPORT_RC_BLOBSTRIPE_GENID;
    }

    if (db->n_rev_constraints) {
        __import_logmsg(LOGMSG_ERROR, "Destination table has reverse constraints\n");
        return COMDB2_IMPORT_RC_REV_CONSTRAINTS;
    }

    /* compare stripe options */
    if (gbl_dtastripe != src_dtastripe ||
        gbl_blobstripe != src_blobstripe) {
        __import_logmsg(LOGMSG_ERROR,
               "stripe settings differ between dst and src. dtastripe: "
               "dst(%d) src(%d) blobstripe: dst(%d) src(%d)\n",
               gbl_dtastripe, src_dtastripe, gbl_blobstripe,
               src_blobstripe);
        return COMDB2_IMPORT_RC_STRIPE_MISMATCH;
    }

    /* success */
    return COMDB2_IMPORT_RC_SUCCESS;
}

static enum comdb2_import_op bulk_import_validate_src_vers_is_supported(const fdb_t * const fdb) {
    const char * src_version = NULL;
    int rc = fdb_get_server_semver(fdb, &src_version);
    if (rc) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed to get version for src db '%s' with rc %d\n",
            fdb_dbname_name(fdb), rc);
        rc = (rc == FDB_ERR_CONNECT) ? COMDB2_IMPORT_RC_NO_SRC_CONN : COMDB2_IMPORT_RC_INTERNAL;
        goto done;
    }

    int cmp_result;
    rc = compare_semvers(src_version, BULK_IMPORT_MIN_SUPPORTED_VERSION, &cmp_result);
    if (rc) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed compare version '%s' for src db '%s' "
            "with min supported version '%s' with rc %d\n",
            src_version, fdb_dbname_name(fdb), BULK_IMPORT_MIN_SUPPORTED_VERSION, rc);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto done;
    }

    const int src_version_is_smaller_than_min = cmp_result < 0;
    if (src_version_is_smaller_than_min) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Version '%s' for src db '%s' "
            "is less than min supported version '%s'\n",
            src_version, fdb_dbname_name(fdb), BULK_IMPORT_MIN_SUPPORTED_VERSION);
        rc = COMDB2_IMPORT_RC_BAD_SRC_VERS;
        goto done;
    }

done:
    if (src_version) { free((char *) src_version); }
    return rc;
}

static enum comdb2_import_op bulk_import_perform_initial_validation(const char *src_db_name,
                                                  const char *dst_table_name) {
    cdb2_hndl_tp *hndl = NULL;
    fdb_t *fdb = NULL;

    int rc = create_local_fdb(src_db_name, &fdb);
    if (rc) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed to create fdb '%s' with rc %d\n",
            src_db_name, rc);
        rc = (rc == FDB_ERR_CLASS_UNKNOWN || rc == FDB_ERR_CLASS_DENIED)
            ? COMDB2_IMPORT_RC_BAD_SRC_CLASS : COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

/* 
 * If we're running an open source build and the source db isn't running a supported
 * version, then we'll fail to run queries on the source db that try to access 
 * unimplemented features. The import will then fail with an internal error.
 *
 * For internal builds we explicitly check the source's version and fail 
 * with a descriptive error on mismatch.
 */
#if defined(LEGACY_DEFAULTS) || defined(COMDB2_TEST)
    rc = bulk_import_validate_src_vers_is_supported(fdb);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
            "Failed version validation for src db '%s'\n", fdb_dbname_name(fdb));
        goto err;
    }
#endif

    rc = cdb2_open(&hndl, fdb_dbname_name(fdb), is_local(fdb) ? "local" : fdb_dbname_class_routing(fdb), 0);
    if (rc) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed to open a handle to src db '%s' with rc %d\n",
            fdb_dbname_name(fdb), rc);
        rc = COMDB2_IMPORT_RC_NO_SRC_CONN;
        goto err;
    }

    rc = cdb2_run_statement(hndl, 
            "select t1.value, t2.value from comdb2_tunables t1, comdb2_tunables t2 \
            where t1.name='dtastripe' and t2.name='blobstripe'")
        || (cdb2_next_record(hndl) != CDB2_OK);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to get tunables from source db.\nerrstr: %s\n",
               cdb2_errstr(hndl));
        rc = (rc == CDB2ERR_CONNECT_ERROR) ? COMDB2_IMPORT_RC_NO_SRC_CONN : COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    const int src_dtastripe = atoi((const char *)cdb2_column_value(hndl, 0));
    const int src_blobstripe = value_on_off((const char *)cdb2_column_value(hndl, 1));

    rdlock_schema_lk();
    rc = bulk_import_data_validate(dst_table_name, src_dtastripe, src_blobstripe, NULL);
    unlock_schema_lk();
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
                "Import data failed validation\n");
        goto err;
    }

err:
    if (fdb) {
        destroy_local_fdb(fdb);
    }
    if (hndl) {
        const int t_rc = cdb2_close(hndl);
        rc = rc ? rc : t_rc;
    }
    return rc;
}

/**
 * Gives the filenames associated with the target table on the foreign db
 * and the new filenames where the table will live on the local db.
 */
static enum comdb2_import_op bulk_import_generate_filenames(
    const ImportData *p_data, const ImportData *p_foreign_data,
    const unsigned long long dst_data_genid,
    const unsigned long long *p_dst_index_genids,
    const unsigned long long *p_dst_blob_genids, char ***p_src_files,
    char ***p_dst_files, int *p_num_files) {
    struct dbtable *db = NULL;
    int dtanum;
    int ixnum;
    int fileix = 0;

    *p_num_files = p_foreign_data->n_index_genids +
                   (p_foreign_data->blobstripe
                        ? p_foreign_data->n_blob_genids * p_data->dtastripe
                        : p_foreign_data->n_blob_genids) +
                   p_data->dtastripe;
    *p_src_files = malloc(sizeof(char *) * (*p_num_files));
    if (!(*p_src_files)) {
        return ENOMEM;
    }

    *p_dst_files = malloc(sizeof(char *) * (*p_num_files));
    if (!(*p_dst_files)) {
        return ENOMEM;
    }

    for (int i = 0; i < (*p_num_files); ++i) {
        (*p_src_files)[i] = (char *)malloc(FILENAMELEN);
        if (!((*p_src_files)[i])) {
            return ENOMEM;
        }
        
        (*p_dst_files)[i] = (char *)malloc(FILENAMELEN);
        if (!((*p_dst_files)[i])) {
            return ENOMEM;
        }
    }

    char **src_files = *p_src_files;
    char **dst_files = *p_dst_files;

    /* find the table we're importing TO */
    if (!(db = get_dbtable_by_name(p_data->table_name))) {
        __import_logmsg(LOGMSG_ERROR, "no such table: %s\n",
               p_data->table_name);
        return COMDB2_IMPORT_RC_NO_DST_TBL;
    }

    /* add data files */
    for (dtanum = 0; dtanum < p_foreign_data->n_blob_genids + 1; dtanum++) {
        int strnum;
        int num_stripes = 0;
        unsigned long long dst_version_num;

        if (dtanum == 0) {
            num_stripes = p_data->dtastripe;
            dst_version_num = dst_data_genid;
        } else {
            if (p_data->blobstripe) {
                num_stripes = p_data->dtastripe;
            } else {
                num_stripes = 1;
            }
            dst_version_num = p_dst_blob_genids[dtanum - 1];
        }

        /* for each stripe add a -file param */
        for (strnum = 0; strnum < num_stripes; ++strnum) {
            /* add src filename */
            if (dtanum == 0) {
                strcpy(src_files[fileix],
                       p_foreign_data->data_files[strnum]);
            } else {
                strcpy(
                    src_files[fileix],
                    p_foreign_data->blob_files[dtanum - 1]->files[strnum]);
            }

            /* add dst filename */
            bdb_form_file_name(db->handle, 1 /*is_data_file*/, dtanum, strnum,
                               dst_version_num, dst_files[fileix], FILENAMELEN);
            fileix++;
        }
    }

    /* for each index add a -file param */
    for (ixnum = 0; ixnum < p_foreign_data->n_index_genids; ixnum++) {
        /* add src filename */
        strcpy(src_files[fileix], p_foreign_data->index_files[ixnum]);

        /* add dst filename */
        bdb_form_file_name(db->handle, 0 /*is_data_file*/, ixnum, 0 /*strnum*/,
                           p_dst_index_genids[ixnum], dst_files[fileix],
                           FILENAMELEN);

        fileix++;
    }

    return COMDB2_IMPORT_RC_SUCCESS;
}

/*
 * Executes a schema change that makes the import target table point 
 * to the imported btree files.
 */
static enum comdb2_import_op bulkimport_switch_files(struct schema_change_type *sc,
                                   struct dbtable *db,
                                   const ImportData *p_foreign_data,
                                   unsigned long long dst_data_genid,
                                   unsigned long long *dst_index_genids,
                                   unsigned long long *dst_blob_genids,
                                   ImportData *local_data, tran_type *tran) {
    int i, bdberr;
    struct ireq iq;

    init_fake_ireq(thedb, &iq);
    iq.usedb = db;

    /* stop the db */
    bdb_lock_table_write(db->handle, tran);

    /* close the table */
    if (bdb_close_only(db->handle, &bdberr)) {
        __import_logmsg(LOGMSG_ERROR,
               "failed to close table: %s bdberr: %d\n",
               p_foreign_data->table_name, bdberr);
        goto err;
    }

    sc->already_finalized = 1;

    llmeta_dump_mapping_table_tran(tran, thedb, db->tablename, 1);

    /* update version for main data files */
    if (bdb_new_file_version_data(db->handle, tran, 0 /*dtanum*/,
                                  dst_data_genid, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        __import_logmsg(LOGMSG_ERROR,
               "failed updating version for table: %s main data "
               "files\n",
               p_foreign_data->table_name);
        goto err;
    }

    /* for each index, update version */
    for (i = 0; i < p_foreign_data->n_index_genids; ++i) {
        /* update version for index */
        if (bdb_new_file_version_index(db->handle, tran, i /*ixnum*/,
                                       dst_index_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            __import_logmsg(LOGMSG_ERROR,
                   "failed updating version for %s's index: %d "
                   "files new version: %llx\n",
                   p_foreign_data->table_name, i,
                   (long long unsigned int)p_foreign_data->index_genids[i]);
            goto err;
        }
    }

    /* for each blob, update version */
    for (i = 0; i < p_foreign_data->n_blob_genids; ++i) {
        /* update version for index */
        if (bdb_new_file_version_data(db->handle, tran, i + 1 /*dtanum*/,
                                      dst_blob_genids[i], &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            __import_logmsg(LOGMSG_ERROR,
                   "failed updating version for %s's blob: %d "
                   "files new version: %llx\n",
                   p_foreign_data->table_name, i,
                   (long long unsigned int)p_foreign_data->blob_genids[i]);
            goto err;
        }
    }

    if (table_version_upsert(db, tran, &bdberr) || bdberr != BDBERR_NOERROR) {
        __import_logmsg(LOGMSG_ERROR,
               "failed to upsert table version bdberr %d\n",
               bdberr);
        goto err;
    }

    bdb_reset_csc2_version(tran, db->tablename, db->schema_version, 1);
    put_db_odh(db, tran, p_foreign_data->odh);
    put_db_compress(db, tran, p_foreign_data->compress);
    put_db_compress_blobs(db, tran, p_foreign_data->compress_blobs);
    put_db_inplace_updates(db, tran, p_foreign_data->ipu);
    put_db_instant_schema_change(db, tran, p_foreign_data->isc);
    put_db_datacopy_odh(db, tran, p_foreign_data->dc_odh);
    for (i = 1; i <= p_foreign_data->n_csc2; ++i) {
        put_csc2_file(db->tablename, tran, i, p_foreign_data->csc2[i - 1]);
    }
    if (p_foreign_data->data_pgsz != -1) {
        bdb_set_pagesize_data(db->handle, tran, p_foreign_data->data_pgsz,
                              &bdberr);
    }
    if (p_foreign_data->index_pgsz != -1) {
        bdb_set_pagesize_index(db->handle, tran, p_foreign_data->index_pgsz,
                               &bdberr);
    }
    if (p_foreign_data->blob_pgsz != -1) {
        bdb_set_pagesize_blob(db->handle, tran, p_foreign_data->blob_pgsz,
                              &bdberr);
    }

    if (reload_after_bulkimport(db, tran)) {
        __import_logmsg(LOGMSG_ERROR, "failed reopening table: %s\n",
               local_data->table_name);
        goto err;
    }

    return COMDB2_IMPORT_RC_SUCCESS;
err:
    return COMDB2_IMPORT_RC_INTERNAL;
}

/*
 * Completes a bulk import by bringing recovered btree files 
 * from the import process into the db's environment and 
 * executing a schema change so that the target table refers to 
 * these new files.
 *
 * p_foreign_data: Pointer to a struct describing the import data.
 * dst_tablename:  Name of local table into which the data 
 *                 is to be imported.
 */
static enum comdb2_import_op bulk_import_complete(struct schema_change_type *sc,
                           ImportData *p_foreign_data,
                           const char *dst_tablename) {
    int offset, num_files, nsiblings, loaded_import_data, have_schema_lk;
    ImportData local_data = IMPORT_DATA__INIT;
    const char *hosts[REPMAX];
    char src_path[PATH_MAX];
    char dst_path[PATH_MAX];
    char **src_files, **dst_files;

    num_files = nsiblings = loaded_import_data = offset = have_schema_lk = 0;
    src_files = dst_files = NULL;

    rdlock_schema_lk();
    have_schema_lk = 1;

    int rc = bulk_import_data_validate(dst_tablename, p_foreign_data->dtastripe, p_foreign_data->blobstripe, NULL);
    if (rc) {
        // keep the rc
        __import_logmsg(LOGMSG_ERROR, "Failed to validate import with rc %d\n",
                              rc);
        goto err;
    }

    rc = bulk_import_data_load(dst_tablename, &local_data, NULL);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "failed getting local data\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }
    loaded_import_data = 1;

    sc->import_dst_data_genid = bdb_get_cmp_context(thedb->bdb_env);

    for (int i = 0; i < p_foreign_data->n_index_genids; ++i)
        sc->import_dst_index_genids[i] = bdb_get_cmp_context(thedb->bdb_env);

    for (int i = 0; i < p_foreign_data->n_blob_genids; ++i)
        sc->import_dst_blob_genids[i] = bdb_get_cmp_context(thedb->bdb_env);

    rc = bulk_import_generate_filenames(&local_data, p_foreign_data,
                                   sc->import_dst_data_genid, sc->import_dst_index_genids,
                                   sc->import_dst_blob_genids, &src_files, &dst_files, &num_files);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to generate filenames with rc %d\n",
                              rc);
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    // We release the schema lock here to avoid holding it while doing scp.
    // We will need to make sure that the table still exists when we reacquire
    // the lock to do the actual schema change.
    unlock_schema_lk();
    have_schema_lk = 0;

    clear_bulk_import_data(&local_data);
    loaded_import_data = 0;

    for (int fileix = 0; fileix < num_files; ++fileix) {
        snprintf(src_path, sizeof(src_path), "%s/%s", p_foreign_data->data_dir,
                 src_files[fileix]);
        snprintf(dst_path, sizeof(dst_path), "%s/%s", thedb->basedir,
                 dst_files[fileix]);

        rc = bdb_bless_btree(src_path, dst_path);
        if (rc) {
            __import_logmsg(LOGMSG_ERROR,
                   "Blessing files failed with rc %d\n",
                   rc);
            rc = COMDB2_IMPORT_RC_INTERNAL;
            goto err;
        }

        nsiblings = net_get_sanctioned_node_list(thedb->handle_sibling,
                                                REPMAX, hosts);
        for (int nodeix = 0; nodeix < nsiblings; ++nodeix) {
            if (gbl_myhostname == hosts[nodeix]) { continue; }

            rc = bulk_import_copy_file_to_replicant(dst_path, hosts[nodeix]);
            if (rc) {
                __import_logmsg(LOGMSG_ERROR,
                       "Failed to copy file to replicant with rc %d\n",
                       rc);
                rc = COMDB2_IMPORT_RC_INTERNAL;
                goto err;
            }
        }
    }

err:
    if (have_schema_lk) {
        unlock_schema_lk();
    }

    if (loaded_import_data) {
        clear_bulk_import_data(&local_data);
    }

    if (num_files != 0) {
        for (int fileix = 0; fileix < num_files; ++fileix) {
            free(src_files[fileix]);
            free(dst_files[fileix]);
        }
        free(src_files);
        free(dst_files);
    }

    return rc;
}

static int comdb2_files_tmpdb_get_file_destination(char *dst_path, ssize_t sz, const char *dir, const char *fname) {
    const int has_dir = strcmp(dir, "") != 0;
    const int rc = has_dir
        ? snprintf(dst_path, sz, "%s/%s/%s", gbl_dbdir, dir, fname)
        : snprintf(dst_path, sz, "%s/%s", gbl_dbdir, fname);

    return rc < 0 || rc >= sz;
}

typedef struct str_list_elt {
    char str[PATH_MAX];
    LINKC_T(struct str_list_elt) lnk;
} str_list_elt;
typedef LISTC_T(str_list_elt) str_list;

/*
 * Concatenate C-Strings `dest` and `src`.
 * The result is stored in `dest`.
 * `dest` must be large enough to store the result of the 
 * concatenation.
 *
 * Returns a pointer to the end of the concatenated C-String.
 * This prevents re-scanning on subsequent calls.
 */
char* strcat_and_get_end( char* dest, char* src )
{
     while (*dest) { dest++; }
     while ((*dest++ = *src++)) {}
     return --dest;
}

static char * generate_select_files_query(cdb2_hndl_tp *hndl, str_list *btree_files_to_fetch) {
    char *query_prefix = "select filename, content, dir from comdb2_files where "
                         "type!='berkdb' or (type='berkdb' and filename in (";
    char *query_suffix = ")) order by filename, offset";
    char *query = malloc(strlen(query_prefix) + strlen(query_suffix) + (listc_size(btree_files_to_fetch)*(PATH_MAX+3)));
    if (!query) { return NULL; }
    char *p = query;
    *query = '\0';

    p = strcat_and_get_end(p, query_prefix);

    str_list_elt *btree_file_elt;
    LISTC_FOR_EACH(btree_files_to_fetch, btree_file_elt, lnk) {
        strcat_and_get_end(p, "'");
        strcat_and_get_end(p, btree_file_elt->str);
        strcat_and_get_end(p, "'");
        if (btree_file_elt->lnk.next) { strcat_and_get_end(p, ","); }
    }

    strcat_and_get_end(p, query_suffix);
    return query;
}

static enum comdb2_import_tmpdb_op comdb2_files_tmpdb_process_filenames(cdb2_hndl_tp *hndl, str_list **filename_list_pp) {
    *filename_list_pp
        = listc_new(offsetof(str_list_elt, lnk));
    str_list * const filename_list_p = *filename_list_pp;
    if (!filename_list_p) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        goto err;
    }

    while (cdb2_next_record(hndl) == CDB2_OK) {
        const char * const fname = (char *)cdb2_column_value(hndl, 0);
        const int fname_len = cdb2_column_size(hndl, 0);

        if (fname_len > PATH_MAX) {
            goto err;
        }

        if (!should_ignore_btree(fname, bulk_import_tmpdb_should_ignore_table, 0, 0)) {
            str_list_elt * const elt = calloc(1, sizeof(str_list_elt));
            if (!elt) {
                __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
                goto err;
            }

            strncpy(elt->str, fname, sizeof(elt->str));
            listc_atl(filename_list_p, elt);
        }
    }

    return COMDB2_IMPORT_TMPDB_RC_SUCCESS;
err:
    if (filename_list_p) {
        LISTC_CLEAN(filename_list_p, lnk, 1, str_list_elt);
        free(filename_list_p);
    }
    return COMDB2_IMPORT_TMPDB_RC_INTERNAL;
}

static enum comdb2_import_tmpdb_op comdb2_files_tmpdb_process_incoming_files(cdb2_hndl_tp *hndl) {
    char * fname = NULL;
    int rc = COMDB2_IMPORT_TMPDB_RC_SUCCESS;
    int fd = -1;

    while (cdb2_next_record(hndl) == CDB2_OK) {
        const char *next_fname = (char *)cdb2_column_value(hndl, 0);
        const char *chunk_content = (char *)cdb2_column_value(hndl, 1);
        const char *dir = (char *)cdb2_column_value(hndl, 2);
        const int chunk_size = cdb2_column_size(hndl, 1);
        char copy_dst[PATH_MAX];

        if (strcmp(next_fname, "checkpoint") == 0) {
            continue;
        }
        if (fname == NULL || strcmp(fname, next_fname) != 0) {
            // We haven't processed this file yet

            if (fname != NULL) {
                free(fname);
            }
            if (fd != -1) {
                Close(fd);
            }

            fname = strdup(next_fname);

            comdb2_files_tmpdb_get_file_destination(copy_dst, sizeof(copy_dst), dir, fname);

            fd = open(copy_dst, O_WRONLY | O_CREAT | O_APPEND, 0755);
            if (fd == -1) {
                __import_logmsg(LOGMSG_ERROR,
                       "Failed to open file %s (errno: %s)\n",
                       copy_dst, strerror(errno));
                rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
                goto err;
            }
        }

        const ssize_t bytes_written = write(fd, chunk_content, chunk_size);
        if (bytes_written != chunk_size) {
            __import_logmsg(LOGMSG_ERROR,
                   "failed to write to the file (expected: %d "
                   "got: %ld)\n", chunk_size, bytes_written);
            rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
            goto err;
        }
    }

err:
    if (fd != -1) {
        Close(fd);
    }

    if (fname != NULL) {
        free(fname);
    }

    return rc;
}

static enum comdb2_import_op symlink_local_files_to_named_fdb_files(const char *fdb_name) {
    static const char *named_file_endings[] =
        {"_file_vers_map", ".metadata.dta", ".llmeta.dta", "" /* delimiter */};
    static const char *unnamed_files[] =
        {"file_vers_map", "comdb2_metadata.dta", "comdb2_llmeta.dta", "" /* delimiter */};

    const char *ending;
    for (int i=0; ((ending = named_file_endings[i]), *ending != '\0'); ++i) {
        char fdb_named_file[FILENAME_MAX];
        snprintf(fdb_named_file, sizeof(fdb_named_file), "%s/%s%s", gbl_dbdir, fdb_name, ending);

        FILE * const f = fopen(fdb_named_file, "w");
        if (!f) {
            __import_logmsg(LOGMSG_ERROR, "fopen failed with errno(%s)\n", strerror(errno));
            goto err;
        }

        int rc = fclose(f);
        if (rc) {
            __import_logmsg(LOGMSG_ERROR, "fclose failed with errno(%s)\n", strerror(errno));
            goto err;
        }

        char local_file[FILENAME_MAX];
        snprintf(local_file, sizeof(local_file), "%s/%s", gbl_dbdir, unnamed_files[i]);

        rc = symlink(fdb_named_file, local_file);
        if (rc) {
            __import_logmsg(LOGMSG_ERROR, "Symlink failed with errno(%s)\n", strerror(errno));
            goto err;
        }
    }

    return COMDB2_IMPORT_RC_SUCCESS;
err:
    return COMDB2_IMPORT_RC_INTERNAL;
}

static enum comdb2_import_op symlink_local_dirs_to_named_fdb_dirs(const char *fdb_name) {
    char local_txndir[PATH_MAX];
    int rc = get_txndir_args(local_txndir, sizeof(local_txndir), gbl_dbdir, gbl_dbname, 1);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to get local txndir");
        goto err;
    }

    char fdb_named_txndir[PATH_MAX];
    rc = get_txndir_args(fdb_named_txndir, sizeof(fdb_named_txndir), gbl_dbdir, fdb_name, 0);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to get remote txndir");
        goto err;
    }

    char local_tmpdir[PATH_MAX];
    rc = comdb2_get_tmp_dir_args(local_tmpdir, sizeof(local_tmpdir), gbl_dbdir, gbl_dbname, 1);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to get local tmpdir");
        goto err;
    }

    char fdb_named_tmpdir[PATH_MAX];
    rc = comdb2_get_tmp_dir_args(fdb_named_tmpdir, sizeof(fdb_named_tmpdir), gbl_dbdir, fdb_name, 0);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to get named tmpdir for fdb %s", fdb_name);
        goto err;
    }

    rc = mkdir(fdb_named_txndir, 0700);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to create directory %s with errno(%s)\n",
            fdb_named_txndir, strerror(errno));
    }

    rc = mkdir(fdb_named_tmpdir, 0700);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed to create directory %s with errno(%s)\n",
            fdb_named_tmpdir, strerror(errno));
    }

    rc = symlink(fdb_named_txndir, local_txndir);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Symlink failed with errno(%s)\n", strerror(errno));
        goto err;
    }

    rc = symlink(fdb_named_tmpdir, local_tmpdir);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Symlink failed with errno(%s)\n", strerror(errno));
        goto err;
    }

    return COMDB2_IMPORT_RC_SUCCESS;
err:
    return COMDB2_IMPORT_RC_INTERNAL;
}

/*
 * Gets foreign db's files and writes them into the local environment.
 */
enum comdb2_import_tmpdb_op bulk_import_tmpdb_pull_foreign_dbfiles(const char *fdb_name) {
    cdb2_hndl_tp *hndl;
    fdb_t *fdb;
    int rc, t_rc;

    rc = t_rc = 0;
    hndl = NULL;

    rc = create_local_fdb(fdb_name, &fdb);
    if (rc) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed to create fdb with rc %d\n",
            rc);
        goto err;
    }

    const char * fdb_dbname = fdb_dbname_name(fdb);

    rc = symlink_local_dirs_to_named_fdb_dirs(fdb_dbname);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
            "Failed to symlink local dirs to named fdb dirs\n");
        goto err;
    }

    rc = symlink_local_files_to_named_fdb_files(fdb_dbname);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
            "Failed to symlink local files to named fdb files\n");
        goto err;
    }

    rc = cdb2_open(&hndl, fdb_dbname_name(fdb), is_local(fdb) ? "local" : fdb_dbname_class_routing(fdb), 0);
    if (rc) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Failed to open a handle to src db with rc %d\n",
            rc);
        goto err;
    }

    rc = cdb2_run_statement(hndl, "exec procedure sys.cmd.send('flush')");
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Got an error flushing src db. errstr: %s\n",
               cdb2_errstr(hndl));
        goto err;
    }
    while (cdb2_next_record(hndl) == CDB2_OK) {}

    rc = cdb2_run_statement(hndl, "set logdelete off");
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Got an error disabling log deletion on src db. errstr: %s\n",
               cdb2_errstr(hndl));
        goto err;
    }
    while (cdb2_next_record(hndl) == CDB2_OK) {}

    rc = cdb2_run_statement(hndl, "select distinct filename from comdb2_filenames where type='berkdb'");
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to get file names from src db. errstr: %s\n",
               cdb2_errstr(hndl));
        goto err;
    }

    str_list *filename_list;
    rc = comdb2_files_tmpdb_process_filenames(hndl, &filename_list);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to process filenames from src db. rc: %d\n",
               rc);
        goto err;
    }

    char *select_files_query = generate_select_files_query(hndl, filename_list);
    if (!select_files_query) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = ENOMEM;
        goto err;
    }

    rc = cdb2_run_statement(hndl, select_files_query);
    free(select_files_query);
    LISTC_CLEAN(filename_list, lnk, 1, str_list_elt);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Got an error grabbing files from src db. errstr: "
               "%s\n",
               cdb2_errstr(hndl));
        goto err;
    }

    rc = comdb2_files_tmpdb_process_incoming_files(hndl);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR,
               "Failed to process files from src db. rc: %d",
               rc);
        goto err;
    }

err:
    if (fdb) {
        destroy_local_fdb(fdb);
    }
    if (hndl) {
        t_rc = cdb2_close(hndl);
        if (t_rc) {
            __import_logmsg(LOGMSG_ERROR,
                   "Failed to close hndl. rc: %d",
                   t_rc);
        }
    }

    rc = rc || t_rc ? COMDB2_IMPORT_TMPDB_RC_INTERNAL : COMDB2_IMPORT_TMPDB_RC_SUCCESS;
    return rc;
}

static void print_blob_files(const BlobFiles *blob_files)
{
    for (int i=0; i<blob_files->n_files; ++i) {
        __import_logmsg(LOGMSG_DEBUG, "File: %s\n", blob_files->files[i]);
    }
}

static void print_import_data(ImportData data)
{
    __import_logmsg(LOGMSG_DEBUG, "ImportData:\n");
    __import_logmsg(LOGMSG_DEBUG, " data_genid: %" PRIu64 "\n", data.data_genid);

    __import_logmsg(LOGMSG_DEBUG, " index_genids:\n");
    for(int i=0; i< data.n_index_genids; i++) {
        __import_logmsg(LOGMSG_DEBUG, "    %" PRIu64 "\n", data.index_genids[i]);
    }

    __import_logmsg(LOGMSG_DEBUG, " blob_genids:\n");
    for(int i=0; i< data.n_blob_genids; i++) {
        __import_logmsg(LOGMSG_DEBUG, "    %" PRIu64 "\n", data.blob_genids[i]);
    }

    __import_logmsg(LOGMSG_DEBUG, " table_name: %s\n:\n", data.table_name);
    __import_logmsg(LOGMSG_DEBUG, " data_dir: %s\n:\n", data.data_dir);
    __import_logmsg(LOGMSG_DEBUG, " csc2_crc32: %" PRIu32 "\n", data.csc2_crc32);
    __import_logmsg(LOGMSG_DEBUG, " checksums: %d\n:\n", data.checksums);
    __import_logmsg(LOGMSG_DEBUG, " odh: %d\n:\n", data.odh);
    __import_logmsg(LOGMSG_DEBUG, " compress: %d\n:\n", data.compress);
    __import_logmsg(LOGMSG_DEBUG, " compress_blobs: %d\n:\n", data.compress_blobs);
    __import_logmsg(LOGMSG_DEBUG, " dtastripe: %d\n:\n", data.dtastripe);
    __import_logmsg(LOGMSG_DEBUG, " blobstripe: %d\n:\n", data.blobstripe);

    __import_logmsg(LOGMSG_DEBUG, " data_files:\n");
    for(int i=0; i< data.n_data_files; i++) {
        __import_logmsg(LOGMSG_DEBUG, "    %s\n", data.data_files[i]);
    }

    __import_logmsg(LOGMSG_DEBUG, " index_files:\n");
    for(int i=0; i< data.n_index_files; i++) {
        __import_logmsg(LOGMSG_DEBUG, "    %s\n", data.index_files[i]);
    }

    __import_logmsg(LOGMSG_DEBUG, " blob_files:\n");
    for (int i=0; i<data.n_blob_files; i++) {
        print_blob_files(data.blob_files[i]);
    }

    __import_logmsg(LOGMSG_DEBUG, " csc2:\n");
    for(int i=0; i< data.n_csc2; i++) {
        __import_logmsg(LOGMSG_DEBUG, "    %s\n", data.csc2[i]);
    }

    __import_logmsg(LOGMSG_DEBUG, " data_pgsz: %d\n:\n", data.data_pgsz);
    __import_logmsg(LOGMSG_DEBUG, " index_pgsz: %d\n:\n", data.index_pgsz);
    __import_logmsg(LOGMSG_DEBUG, " blob_pgsz: %d\n:\n", data.blob_pgsz);
    __import_logmsg(LOGMSG_DEBUG, " ipu: %d\n:\n", data.ipu);
    __import_logmsg(LOGMSG_DEBUG, " isc: %d\n:\n", data.isc);
    __import_logmsg(LOGMSG_DEBUG, " dc_odh: %d\n:\n", data.dc_odh);

}

/*
 * Gets this process's executable
 */
static enum comdb2_import_op get_my_comdb2_executable(char **p_exe)
{
    int size;
    pid_t pid;

    size = 0;
    pid = getpid();

#if defined(_LINUX_SOURCE)
    size = snprintf(NULL, 0, "/proc/%ld/exe", (long) pid);
    *p_exe = malloc(++size);
    if (*p_exe == NULL) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        goto err;
    }
    sprintf(*p_exe, "/proc/%ld/exe", (long) pid);
#elif defined(_SUN_SOURCE)
    size = snprintf(NULL, 0, "/proc/%ld/execname", (long) pid);
    *p_exe = malloc(++size);
    if (*p_exe == NULL) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        goto err;
    }
    sprintf(*p_exe, "/proc/%ld/execname", (long) pid);
#else
    #error "Unsupported platform"
#endif

    return COMDB2_IMPORT_RC_SUCCESS;
err:
    return COMDB2_IMPORT_RC_INTERNAL;
}

const char *bulk_import_get_err_str(const int rc) {
    switch (rc) {
        case COMDB2_IMPORT_RC_NO_DST_TBL:
            return "Destination table does not exist";
        case COMDB2_IMPORT_RC_NO_SRC_TBL:
            return "Source table does not exist";
        case COMDB2_IMPORT_RC_BLOBSTRIPE_GENID:
            return "Destination table has a blobstripe genid";
        case COMDB2_IMPORT_RC_CONSTRAINTS:
            return "Source table has constraints";
        case COMDB2_IMPORT_RC_REV_CONSTRAINTS:
            return "Destination table has reverse constraints";
        case COMDB2_IMPORT_RC_NO_ISC_OPTION:
            return "Source table has no ISC option";
        case COMDB2_IMPORT_RC_NO_IPU_OPTION:
            return "Source table has no IPU option";
        case COMDB2_IMPORT_RC_NO_ODH_OPTION:
            return "Source table has no ODH option";
        case COMDB2_IMPORT_RC_STRIPE_MISMATCH:
            return "Stripe settings differ between source and destination db";
        case COMDB2_IMPORT_RC_NO_SRC_CONN:
            return "Unable to establish a connection to source db";
        case COMDB2_IMPORT_RC_BAD_SRC_CLASS:
            return "Can't connect to provided source db class";
        case COMDB2_IMPORT_RC_BAD_SRC_VERS:
            return "Source db version is less than min required for import";
        case COMDB2_IMPORT_RC_INTERNAL:
            return "An internal error occurred";
        case COMDB2_IMPORT_RC_UNKNOWN:
        default:
            return "An unknown error occurred";
    }
}

static enum comdb2_import_op get_import_rcode_from_tmpdb_rcode(const int rc) {
    switch (rc) {
        case COMDB2_IMPORT_TMPDB_RC_SUCCESS:
            return COMDB2_IMPORT_RC_SUCCESS;
        case COMDB2_IMPORT_TMPDB_RC_CONSTRAINTS:
            return COMDB2_IMPORT_RC_CONSTRAINTS;
        case COMDB2_IMPORT_TMPDB_RC_NO_ISC_OPTION:
            return COMDB2_IMPORT_RC_NO_ISC_OPTION;
        case COMDB2_IMPORT_TMPDB_RC_NO_IPU_OPTION:
            return COMDB2_IMPORT_RC_NO_IPU_OPTION;
        case COMDB2_IMPORT_TMPDB_RC_NO_ODH_OPTION:
            return COMDB2_IMPORT_RC_NO_ODH_OPTION;
        case COMDB2_IMPORT_TMPDB_RC_NO_SRC_TBL:
            return COMDB2_IMPORT_RC_NO_SRC_TBL;
        case COMDB2_IMPORT_TMPDB_RC_INTERNAL:
            return COMDB2_IMPORT_RC_INTERNAL;
        default:
            return COMDB2_IMPORT_RC_UNKNOWN;
    }
}

int do_import(struct ireq *iq, struct schema_change_type *sc, tran_type *tran)
{
    const char * const src_tablename = sc->import_src_tablename;
    const char * const srcdb = sc->import_src_dbname;
    const char * const dst_tablename = sc->tablename;

    char *tmp_db_dir = NULL;
    char *command = NULL;
    char *exe = NULL;

    pthread_mutex_lock(&import_id_mutex);
    const uint64_t import_id = gbl_import_id++;
    pthread_mutex_unlock(&import_id_mutex);

    int rc = bulk_import_perform_initial_validation(srcdb, dst_tablename);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "Failed initial validation. Aborting import.\n");
        goto err;
    }

    rc = bulk_import_setup_import_db(&tmp_db_dir, import_id, srcdb);
    if (rc) {
        assert(rc == COMDB2_IMPORT_RC_INTERNAL);
        __import_logmsg(LOGMSG_ERROR, "Failed to setup import db\n");
        goto err;
    }

    rc = get_my_comdb2_executable(&exe);
    if (rc != 0) {
        assert(rc == COMDB2_IMPORT_RC_INTERNAL);
        __import_logmsg(LOGMSG_ERROR, "Failed to get my comdb2 executable\n");
        goto err;
    }

    const char *my_tier = get_my_mach_class_str();
    const int cmd_size = snprintf(NULL, 0,
        "%s --import --lrl %s/import.lrl --dir %s --tables %s --src %s --my-tier %s",
        exe, tmp_db_dir, tmp_db_dir, src_tablename, srcdb, my_tier)
        + 1;
    command = malloc(cmd_size);
    if (command == NULL) {
        rc = COMDB2_IMPORT_RC_INTERNAL;
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        goto err;
    }

    sprintf(command,
        "%s --import --lrl %s/import.lrl --dir %s --tables %s --src %s --my-tier %s",
         exe, tmp_db_dir, tmp_db_dir, src_tablename, srcdb, my_tier);

    rc = system(command);
    if (rc != 0) {
        rc = WIFEXITED(rc)
            ? get_import_rcode_from_tmpdb_rcode(WEXITSTATUS(rc))
            : COMDB2_IMPORT_RC_INTERNAL; 
        __import_logmsg(LOGMSG_ERROR, "Import process failed.\n");
        goto err;
    }
    
    rc = bulk_import_data_unpack_from_file(&sc->import_src_table_data, import_id);
    if (rc != 0) {
        assert(rc == COMDB2_IMPORT_RC_INTERNAL);
        __import_logmsg(LOGMSG_ERROR, "Failed to unpack import data\n");
        goto err;
    }
    rc = bulk_import_complete(sc, sc->import_src_table_data, dst_tablename);
    if (rc != 0) {
        __import_logmsg(LOGMSG_ERROR, "Failed to complete import.\n");
        goto err;
    }

err:
    if (command) {
        free(command);
    }

    if (exe) {
        free(exe);
    }

    if (rc && sc->import_src_table_data) {
        import_data__free_unpacked(sc->import_src_table_data, &pb_alloc);
        sc->import_src_table_data = NULL;
    }

    if (tmp_db_dir) {
        const int t_rc = bulk_import_cleanup_import_db(tmp_db_dir);
        if (t_rc != 0) {
            // Don't error here: We've can still finish the import successfully.
            __import_logmsg(LOGMSG_WARN, "Cleaning up import db failed with rc %d. "
                "Future imports may fail if this doesn't get cleaned up.\n", t_rc);
        }
    }

    if (rc) {
        errstat_set_rcstrf(&iq->errstat, rc, bulk_import_get_err_str(rc));
    }
    return rc;
}

int finalize_import(struct ireq *iq, struct schema_change_type *sc, tran_type *tran)
{
    if (!tran) {
        __import_logmsg(LOGMSG_FATAL, "Expected tran to be non-null. Aborting\n");
        abort();
    }

    const char * const dst_tablename = sc->tablename;
    unsigned long long dst_data_genid = sc->import_dst_data_genid;
    unsigned long long * const dst_index_genids = sc->import_dst_index_genids;
    unsigned long long * const dst_blob_genids = sc->import_dst_blob_genids;
    const ImportData * const p_foreign_data = sc->import_src_table_data;
    int loaded_import_data = 0;

    ImportData local_data = IMPORT_DATA__INIT;

    // This is where everything actually happens. We need the schema lock in write mode.

    // Need to redo validation: blobstripe genid could change live
    int rc = bulk_import_data_validate(dst_tablename, p_foreign_data->dtastripe, p_foreign_data->blobstripe, tran);
    if (rc) {
        // keep the rc
        __import_logmsg(LOGMSG_ERROR, "Failed to validate import with rc %d\n",
                              rc);
        goto err;
    }

    struct dbtable * const db = get_dbtable_by_name(dst_tablename);
    if (!db) {
        __import_logmsg(LOGMSG_ERROR, "no such table: %s\n",
               dst_tablename);
        // This is an internal error: We just passed validation while holding
        // the schema lock, so the table should exist.
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }

    rc = bulk_import_data_load(dst_tablename, &local_data, tran);
    if (rc) {
        __import_logmsg(LOGMSG_ERROR, "failed getting local data\n");
        rc = COMDB2_IMPORT_RC_INTERNAL;
        goto err;
    }
    loaded_import_data = 1;

    if (gbl_debug_sleep_during_bulk_import) { sleep(5); }

    rc = bulkimport_switch_files(sc, db, p_foreign_data, dst_data_genid,
                                dst_index_genids, dst_blob_genids, &local_data, tran);

err:
    if (loaded_import_data) {
        clear_bulk_import_data(&local_data);
    }

    import_data__free_unpacked(sc->import_src_table_data, &pb_alloc);
    sc->import_src_table_data = NULL;

    if (rc) {
        errstat_set_rcstrf(&iq->errstat, rc, bulk_import_get_err_str(rc));
    }

    return rc;
}

/*
 * Packs bulk import data about a table to a file.
 *
 * import_table: The name of the table
 */
enum comdb2_import_tmpdb_op bulk_import_tmpdb_write_import_data(const char *import_table) {
    int rc;
    int written_bytes;
    unsigned len;
    void *buf;
    FILE *f_bulk_import;
    char import_file_path[PATH_MAX];
    ImportData import_data = IMPORT_DATA__INIT;

    len = written_bytes = 0;
    buf = NULL;
    f_bulk_import = NULL;

    const struct dbtable *db = get_dbtable_by_name(import_table);
    if (!db) {
        __import_logmsg(LOGMSG_ERROR, "no such table: %s\n",
               import_table);
        rc = COMDB2_IMPORT_TMPDB_RC_NO_SRC_TBL;
        goto err;
    }

    if (db->n_constraints) {
        __import_logmsg(LOGMSG_ERROR,
               "Importing tables with constraints is not supported\n");
        rc = COMDB2_IMPORT_TMPDB_RC_CONSTRAINTS;
        goto err;
    }

    rc = bulk_import_data_load(import_table, &import_data, NULL);
    if (rc != 0) {
        __import_logmsg(LOGMSG_FATAL, "Failed to load import data\n");
        rc = (rc == COMDB2_IMPORT_RC_INTERNAL) ? COMDB2_IMPORT_TMPDB_RC_INTERNAL : rc;
        goto err;
    }

    rc = bulk_import_get_import_data_fname(import_file_path, sizeof(import_file_path), 0);
    if (rc != 0) {
        __import_logmsg(LOGMSG_ERROR, "Failed to get import data fname\n");
        rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
        goto err;
    }

    f_bulk_import = fopen(import_file_path, "w");
    if (f_bulk_import == NULL) {
        __import_logmsg(LOGMSG_ERROR, "Failed to open file %s. err %s\n",
               import_file_path, strerror(errno));
        rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
        goto err;
    }

    len = import_data__get_packed_size(&import_data);
    buf = malloc(len);
    if (buf == NULL) {
        __import_logmsg(LOGMSG_ERROR, "Could not allocate memory\n");
        rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
        goto err;
    }

    written_bytes = import_data__pack(&import_data, buf);
    if (written_bytes != len) {
        __import_logmsg(LOGMSG_ERROR,
               "Did not pack full protobuf buffer.\n");
        rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
        goto err;
    }

    written_bytes = fwrite(buf, 1, len, f_bulk_import);
    if (written_bytes != len) {
        __import_logmsg(
            LOGMSG_ERROR,
            "Did not write full protobuf buffer to file");
        rc = COMDB2_IMPORT_TMPDB_RC_INTERNAL;
        goto err;
    }

err:
    if (f_bulk_import != NULL) {
        fclose(f_bulk_import);
    }

    if (buf != NULL) {
        free(buf);
    }

    return rc;
}
