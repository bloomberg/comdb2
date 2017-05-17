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
#include "schemachange_int.h"
#include "sc_net.h"
#include "intern_strings.h"

static const char *delims = " \n\r\t";
int gbl_commit_sleep;
int gbl_convert_sleep;

int do_dryrun(struct schema_change_type *sc)
{
    SBUF2 *sb = sc->sb; 
    int rc;
    if (sc_set_running(1, get_genid(thedb->bdb_env, 0), gbl_mynode,
                time(NULL)) == 0) {
        rc = dryrun(sc);
        sc_set_running(0, sc_seed, gbl_mynode, time(NULL));
    } else {
        sbuf2printf(sb, "!schema change already in progress\n");
        sbuf2printf(sb, "FAILED\n");
        rc = -1;
    }
    free_schema_change_type(sc);
    return rc;
}


int appsock_schema_change(SBUF2 *sb, int *keepsocket)
{

#ifdef DEBUG
    printf("%s: entering\n", __func__);
#endif

    char line[1024];
    int rc;
    char *schemabuf = NULL;
    int buflen = 0;
    int bufpos = 0;
    char *lasts = NULL;
    char *tok = NULL;
    int noschema = 0;

    struct schema_change_type sc = {0};
    sc.type = DBTYPE_TAGGED_TABLE;
    sc.sb = sb;
    sc.nothrevent = 0;
    sc.onstack = 1;
    sc.live = 0;
    sc.drop_table = 0;
    sc.use_plan = 0;
    /* default values: no change */
    sc.headers = 1;
    sc.compress = -1;
    sc.compress_blobs = -1;
    sc.ip_updates = -1;
    sc.instant_sc = -1;
    sc.commit_sleep = gbl_commit_sleep;
    sc.convert_sleep = gbl_convert_sleep;
    sc.dbnum = -1; /* -1 = not changing, anything else = set value */
    listc_init(&sc.dests, offsetof(struct dest, lnk));

    sc.finalize = 1;
    /* DRQS 21247361 - don't allow schema change to run if kicked off from
       a machine from which we don't allow writes (eg dev to alpha) */
    if (gbl_check_schema_change_permissions && sc_request_disallowed(sb)) {
        sbuf2printf(sb, "!Schema change not allowed from this machine (writes "
                        "not allowed on remote)\n");
        sbuf2printf(sb, "FAILED\n");
        return -1;
    }
    // backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
    sc.scanmode = gbl_default_sc_scanmode;

    if (gbl_default_livesc)
        sc.live = 1;
    if (gbl_default_plannedsc)
        sc.use_plan = 1;

    /* First, reject if i am not master */
    if (thedb->master != gbl_mynode) {
        sbuf2printf(sb, "!I am not master, master is %d\n", thedb->master);
        sbuf2printf(sb, "FAILED\n");
        return -1;
    }

    /* Then, read options */
    rc = sbuf2gets(line, sizeof(line), sb);
    if (rc < 0) {
        fprintf(stderr, "appsock schema change I/O error reading options\n");
        return -1;
    }

    tok = strtok_r(line, delims, &lasts);
    while (tok) {

#ifdef DEBUG
        printf("%s: parameter '%s'\n", __func__, tok);
#endif

        if (strcmp(tok, "add") == 0)
            sc.addonly = 1;
        else if (strcmp(tok, "upgrade") == 0)
            sc.fulluprecs = 1;
        else if (strcmp(tok, "addsp") == 0)
            sc.addsp = 1;
        else if (strcmp(tok, "delsp") == 0)
            sc.delsp = 1;
        else if (strcmp(tok, "defaultsp") == 0)
            sc.defaultsp = 1;
        else if (strcmp(tok, "showsp") == 0)
            sc.showsp = 1;
        else if (strcmp(tok, "alter") == 0)
            sc.alteronly = 1;
        else if (strcmp(tok, "live") == 0)
            sc.live = 1;
        else if (strcmp(tok, "nolive") == 0)
            sc.live = 0;
        else if (strcmp(tok, "parallelscan") == 0)
            sc.scanmode = SCAN_PARALLEL;
        else if (strcmp(tok, "dumpscan") == 0)
            sc.scanmode = SCAN_DUMP;
        else if (strcmp(tok, "indexscan") == 0)
            sc.scanmode = SCAN_INDEX;
        else if (strncmp(tok, "table:", 6) == 0)
            strncpy0(sc.table, tok + 6, sizeof(sc.table));
        else if (strcmp(tok, "fullrebuild") == 0)
            sc.force_rebuild = 1;
        else if (strcmp(tok, "rebuildindex") == 0)
            sc.rebuild_index = 1;
        else if (strcmp(tok, "rebuilddata") == 0)
            sc.force_dta_rebuild = 1;
        else if (strcmp(tok, "rebuildblobsdata") == 0) {
            sc.force_dta_rebuild = 1;
            sc.force_blob_rebuild = 1;
        } else if (strcmp(tok, "ignoredisk") == 0 || strcmp(tok, "force") == 0)
            sc.force = 1;
        else if (strcmp(tok, "add_headers") == 0)
            sc.headers = 1;
        else if (strcmp(tok, "remove_headers") == 0)
            sc.headers = 0;
        else if (strcmp(tok, "ipu_on") == 0)
            sc.ip_updates = 1;
        else if (strcmp(tok, "ipu_off") == 0)
            sc.ip_updates = 0;
        else if (strcmp(tok, "instant_sc_on") == 0)
            sc.instant_sc = 1;
        else if (strcmp(tok, "instant_sc_off") == 0)
            sc.instant_sc = 0;

        else if (strcmp(tok, "rec_zlib") == 0)
            sc.compress = BDB_COMPRESS_ZLIB;
        else if (strcmp(tok, "rec_rle8") == 0)
            sc.compress = BDB_COMPRESS_RLE8;
        else if (strcmp(tok, "rec_crle") == 0)
            sc.compress = BDB_COMPRESS_CRLE;
        else if (strcmp(tok, "rec_lz4") == 0)
            sc.compress = BDB_COMPRESS_LZ4;
        else if (strcmp(tok, "rec_nocompress") == 0)
            sc.compress = BDB_COMPRESS_NONE;

        else if (strcmp(tok, "blob_zlib") == 0)
            sc.compress_blobs = BDB_COMPRESS_ZLIB;
        else if (strcmp(tok, "blob_rle8") == 0)
            sc.compress_blobs = BDB_COMPRESS_RLE8;
        else if (strcmp(tok, "blob_lz4") == 0)
            sc.compress_blobs = BDB_COMPRESS_LZ4;
        else if (strcmp(tok, "blob_nocompress") == 0)
            sc.compress_blobs = BDB_COMPRESS_NONE;

        else if (strcmp(tok, "useplan") == 0)
            sc.use_plan = 1;
        else if (strcmp(tok, "noplan") == 0)
            sc.use_plan = 0;
        else if (strcmp(tok, "noschema") == 0)
            noschema = 1;
        else if (strcmp(tok, "fastinit") == 0)
            sc.fastinit = 1;
        else if (strcmp(tok, "drop") == 0) {
            sc.fastinit = 1;
            sc.drop_table = 1;
        } else if (strncmp(tok, "aname:", 6) == 0)
            strncpy0(sc.aname, tok + 6, sizeof(sc.aname));
        else if (strncmp(tok, "commitsleep:", 12) == 0)
            sc.commit_sleep = atoi(tok + 12);
        else if (strncmp(tok, "convsleep:", 10) == 0)
            sc.convert_sleep = atoi(tok + 10);
        else if (strcmp(tok, "dryrun") == 0)
            sc.dryrun = 1;
        else if (strcmp(tok, "statistics") == 0)
            sc.statistics = 1;
        else if (strncmp(tok, "dbnum:", 6) == 0)
            sc.dbnum = atoi(tok + 6);
        else if (strcmp(tok, "trigger") == 0)
            sc.is_trigger = 1;
        else if (strncmp(tok, "dest:", 5) == 0) {
            struct dest *d;
            d = malloc(sizeof(struct dest));
            /* TODO: check for dupes here? */
            if (d == NULL) {
                fprintf(stderr, "%s: malloc can't allocate %d bytes\n",
                        __func__, sizeof(struct dest));
                return -1;
            }
            d->dest = strdup(tok + 5);
            if (d->dest == NULL) {
                fprintf(
                    stderr,
                    "%s: malloc can't allocate %d bytes for destination name\n",
                    __func__, strlen(tok + 5));
                free(d);
                return -1;
            }
            listc_abl(&sc.dests, d);
        } else {
            sbuf2printf(sb, "!unknown option '%s'\n", tok);
            sbuf2printf(sb, "FAILED\n");
            return -1;
        }
        tok = strtok_r(NULL, delims, &lasts);
    }

#ifdef DEBUG
    printf("%s:  sc.table '%s'\n", __func__, sc.table);
    printf("%s:  sc.aname '%s'\n", __func__, sc.aname);
#endif

    if (sc.table[0] == '\0' && !sc.showsp) {
        sbuf2printf(sb, "!no table name specified\n");
        sbuf2printf(sb, "FAILED\n");
        return -1;
    }

    /* This gets overridden later if it's needed */
    sc.fname[0] = '\0';

    /* Now, read new schema */
    while ((rc = sbuf2gets(line, sizeof(line), sb)) > 0) {
        if (strcmp(line, ".\n") == 0) {
            rc = 0;
            break;
        } else if (!noschema) {
            void *newp;
            int len = strlen(line) + 1; /* include \0 terminator */
            if (buflen - bufpos < len) {
                int newlen = buflen + sizeof(line) * 16;
                if (schemabuf)
                    newp = realloc(schemabuf, newlen);
                else
                    newp = malloc(newlen);
                if (!newp) {
                    fprintf(stderr, "appsock_schema_change_inner: out of "
                                    "memory buflen=%d\n",
                            buflen);
                    cleanup_strptr(&schemabuf);
                    sbuf2printf(sb, "!out of memory reading schema\n");
                    sbuf2printf(sb, "FAILED\n");
                    return -1;
                }
                schemabuf = newp;
                buflen = newlen;
            }
            memcpy(schemabuf + bufpos, line, len);
            bufpos += len - 1; /* don't add \0 terminator */
        }
    }
    /* we KNOW that comdb2sc adds a new line to be able to use the .<NL> trick;
       remove it HERE
     */
    if (bufpos > 0 && schemabuf[bufpos - 1] == '\n') {
        schemabuf[bufpos - 1] = '\0';
    }

    if (rc < 0) {
        fprintf(stderr, "appsock schema change I/O error reading schema\n");
        cleanup_strptr(&schemabuf);
        return -1;
    }

    if (sc.addsp) {
        sc.newcsc2 = schemabuf;
        return do_add_sp(&sc, NULL);
    } else if (sc.delsp) {
        sc.newcsc2 = schemabuf;
        return do_del_sp(&sc, NULL);
    } else if (sc.defaultsp) {
        sc.newcsc2 = schemabuf;
        return do_default_sp(&sc, NULL);
    } else if (sc.showsp) {
        sc.newcsc2 = schemabuf;
        return do_show_sp(&sc);
    } else if (sc.is_trigger) {
        sc.newcsc2 = schemabuf;
        return perform_trigger_update(&sc);
    }

    if (noschema) {
        /* Find the existing table and use its current schema */
        struct db *db;
        db = getdbbyname(sc.table);
        if (db == NULL) {
            sbuf2printf(sb, "!table '%s' not found\n", sc.table);
            sbuf2printf(sb, "FAILED\n");
            cleanup_strptr(&schemabuf);
            return -1;
        }

        if (get_csc2_file(db->dbname, -1 /*highest csc2_version*/, &schemabuf,
                          NULL /*csc2len*/)) {
            fprintf(stderr, "%s: could not get schema for table: %s\n",
                    __func__, db->dbname);
            cleanup_strptr(&schemabuf);
            return -1;
        }

        if (sc.rebuild_index) {
            int indx;
            int rc = getidxnumbyname(sc.table, sc.aname, &indx);
            if (rc) {
                sbuf2printf(sb, "!table:index '%s:%s' not found\n", sc.table,
                            sc.aname);
                sbuf2printf(sb, "FAILED\n");
                cleanup_strptr(&schemabuf);
                return -1;
            }
            sc.index_to_rebuild = indx;
        }

        sc.same_schema = 1;
    }
    sc.newcsc2 = schemabuf;

    /* We have our options and a new schema.  Do the schema change.  If we
     * get called from an appsock pool thread (which has a small stack) then
     * make a new thread for this so that we have plenty of room to work. */
    if (sc.dryrun) {
        return do_dryrun(&sc);
    }

    /* Print status of a table */
    if (sc.statistics) {
        rc = print_status(&sc);
        cleanup_strptr(&schemabuf);
        return rc;
    } else {
        struct schema_change_type *s = malloc(sizeof(sc));
        if (!s) {
            sbuf2printf(sb, "!out of memory in database\n");
            sbuf2printf(sb, "FAILED\n");
            cleanup_strptr(&schemabuf);
            return -1;
        }
        memcpy(s, &sc, sizeof(sc));

        s->onstack = 0;
        if (sc.fastinit) {
            s->nothrevent = 1;
        } else {
            s->must_close_sb = 1;
            *keepsocket = 1; /* we now own the socket */
            s->nothrevent = 0;
        }
        rc = start_schema_change(thedb, s, NULL);

        if (rc != SC_OK && rc != SC_ASYNC) {
            return -1;
        }
    }

    return 0;
}

void handle_setcompr(SBUF2 *sb)
{
    int rc;
    struct db *db;
    struct ireq iq;
    char line[256];
    char *tok, *saveptr;
    const char *tbl = NULL, *rec = NULL, *blob = NULL;

    if ((rc = sbuf2gets(line, sizeof(line), sb)) < 0) {
        fprintf(stderr, "%s -- sbuf2gets rc: %d\n", __func__, rc);
        return;
    }
    if ((tok = strtok_r(line, delims, &saveptr)) == NULL) {
        sbuf2printf(sb, ">Bad arguments\n");
        goto out;
    }
    do {
        if (strcmp(tok, "tbl") == 0)
            tbl = strtok_r(NULL, delims, &saveptr);
        else if (strcmp(tok, "rec") == 0)
            rec = strtok_r(NULL, delims, &saveptr);
        else if (strcmp(tok, "blob") == 0)
            blob = strtok_r(NULL, delims, &saveptr);
        else {
            sbuf2printf(sb, ">Bad arguments\n");
            goto out;
        }
    } while ((tok = strtok_r(NULL, delims, &saveptr)) != NULL);

    if (rec == NULL && blob == NULL) {
        sbuf2printf(sb, ">No compression operation specified\n");
        goto out;
    }
    if ((db = getdbbyname(tbl)) == NULL) {
        sbuf2printf(sb, ">Table not found: %s\n", tbl);
        goto out;
    }
    if (!db->odh) {
        sbuf2printf(sb, ">Table isn't ODH\n");
        goto out;
    }

    init_fake_ireq(thedb, &iq);
    iq.usedb = db;
    iq.sb = sb;

    wrlock_schema_lk();
    rc = do_setcompr(&iq, rec, blob);
    unlock_schema_lk();

    if (rc == 0)
        sbuf2printf(sb, "SUCCESS\n");
    else
    out:
    sbuf2printf(sb, "FAILED\n");

    sbuf2flush(sb);
}

void vsb_printf(SBUF2 *sb, const char *sb_prefix, const char *prefix,
                const char *fmt, va_list args)
{
    char line[1024];
    char *s;
    char *next;

    vsnprintf(line, sizeof(line), fmt, args);
    s = line;
    while ((next = strchr(s, '\n'))) {
        *next = 0;

        if (sb) {
            sbuf2printf(sb, "%s%s\n", sb_prefix, s);
            sbuf2flush(sb);
        }
        logmsg(LOGMSG_INFO, "%s%s\n", prefix, s);
        ctrace("%s%s\n", prefix, s);

        s = next + 1;
    }
    if (*s) {
        if (sb) {
            sbuf2printf(sb, "%s%s", sb_prefix, s);
            sbuf2flush(sb);
        }

        printf("%s%s\n", prefix, s);
        ctrace("%s%s\n", prefix, s);
    }
}

void sb_printf(SBUF2 *sb, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    vsb_printf(sb, "?", "", fmt, args);

    va_end(args);
}

void sb_errf(SBUF2 *sb, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    vsb_printf(sb, "!", "", fmt, args);

    va_end(args);
}

void sc_printf(struct schema_change_type *s, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    if (s->partialuprecs) {
        va_end(args);
        return;
    }

    if (s && s->sb)
        pthread_mutex_lock(&schema_change_sbuf2_lock);

    vsb_printf((s) ? s->sb : NULL, "?", "Schema change info: ", fmt, args);

    if (s && s->sb)
        pthread_mutex_unlock(&schema_change_sbuf2_lock);

    va_end(args);
}

void sc_errf(struct schema_change_type *s, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);

    if (s->partialuprecs) {
        va_end(args);
        return;
    }

    if (s && s->sb)
        pthread_mutex_lock(&schema_change_sbuf2_lock);

    vsb_printf((s) ? s->sb : NULL, "!", "Schema change error: ", fmt, args);

    if (s && s->sb)
        pthread_mutex_unlock(&schema_change_sbuf2_lock);

    va_end(args);
}

