#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "comdb2.h"
#include "schemachange.h"
#include "sc_lua.h"
#include "sc_queues.h"
#include "sqlglue.h"
#include "translistener.h"
#include "bdb_int.h"

#include "logmsg.h"

int gbl_fail_to_create_default_cons = 0;
int gbl_create_default_consumer_atomically = 1;

struct sp_file_t {
    char name[128]; // MAX_SPNAME
    int default_version;
    int version;
    int size;
};

struct sp_versioned_file_t {
    char name[128];
    char version[80]; // MAX_SPVERSION_LEN
    int is_default;
    int size;
};

int dump_user_version_spfile(const char *file)
{
    char **cname = NULL;
    int ccnt = 0;
    int has_sp = 0;

    int fd_out = -1;
    COMDB2BUF *sb_out = NULL;

    bdb_get_versioned_sps_tran(NULL, &cname, &ccnt);
    if (ccnt <= 0) {
        return 0;
    }

    logmsg(LOGMSG_USER, "FILE : %s\n", file);
    fd_out = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd_out == -1) {
        logmsg(LOGMSG_USER, "%s: cannot open '%s' for writing %d %s\n", __func__, file, errno, strerror(errno));
        goto cleanup;
    }
    sb_out = cdb2buf_open(fd_out, 0);
    if (!sb_out) {
        Close(fd_out);
        goto cleanup;
    }

    for (int i = 0; i < ccnt; i++) {
        char **cvers = NULL;
        char *def = NULL;
        int vcnt = 0;
        bdb_get_all_for_versioned_sp(cname[i], &cvers, &vcnt);
        bdb_get_default_versioned_sp(cname[i], &def);
        for (int j = 0; j < vcnt; j++) {
            has_sp = 1;
            char *src = NULL;
            int size;
            bdb_get_versioned_sp(cname[i], cvers[j], &src);
            size = src ? strlen(src) + 1 : 0;

            struct sp_versioned_file_t sp_ver_f = {0};
            strcpy(sp_ver_f.name, cname[i]);
            strcpy(sp_ver_f.version, cvers[j]);
            if (def && !strcmp(def, cvers[j])) {
                sp_ver_f.is_default = 1;
            }
            sp_ver_f.size = size;
            cdb2buf_write((char *)&sp_ver_f, sizeof(sp_ver_f), sb_out);
            cdb2buf_write(src, size, sb_out);
            free(src);
            free(cvers[j]);
        }
        free(cvers);
        free(def);
    }

cleanup:
    for (int x = 0; x < ccnt; x++) {
        free(cname[x]);
    }
    free(cname);

    if (sb_out) {
        cdb2buf_close(sb_out);
    }

    return has_sp;
}

int read_user_version_spfile(const char *file)
{
    int fd_in = open(file, O_RDONLY);
    if (fd_in == -1) {
        return -1;
    }

    COMDB2BUF *sb_in = cdb2buf_open(fd_in, 0);
    if (!sb_in) {
        Close(fd_in);
        return -1;
    }

    while (1) {
        struct sp_versioned_file_t sp_ver_f = {0};
        if (sizeof(sp_ver_f) != cdb2buf_fread((char *)&sp_ver_f, 1, sizeof(sp_ver_f), sb_in)) {
            cdb2buf_close(sb_in);
            return 0;
        }
        if (sp_ver_f.size) {
            char *src = malloc(sp_ver_f.size);
            if (sp_ver_f.size != cdb2buf_fread(src, 1, sp_ver_f.size, sb_in)) {
                free(src);
                cdb2buf_close(sb_in);
                return -1;
            }
            bdb_add_versioned_sp(NULL, sp_ver_f.name, sp_ver_f.version, src);
            if (sp_ver_f.is_default) {
                bdb_set_default_versioned_sp(NULL, sp_ver_f.name, sp_ver_f.version);
            }
            free(src);
        }
    }
    cdb2buf_close(sb_in);
    return 0;
}

int dump_spfile(const char *file)
{
    char old_sp[32];
    char new_sp[32];
    old_sp[0] = 127;
    int bdberr, rc = 0;
    int has_sp = 0;
    int fd_out = -1;
    COMDB2BUF *sb_out = NULL;

    while (rc == 0) {
        rc = bdb_get_sp_name(NULL, old_sp, new_sp, &bdberr);
        if (rc || (strcmp(old_sp, new_sp) <= 0)) {
            cdb2buf_close(sb_out);
            if (has_sp) return 1;

            return 0;
        }

        if (!has_sp) {
            /* Open the file to store stored procedures. */
            has_sp = 1;
            logmsg(LOGMSG_USER, "FILE : %s\n", file);
            fd_out = open(file, O_WRONLY | O_CREAT | O_TRUNC, 0666);
            if (fd_out == -1) {
                logmsg(LOGMSG_USER, "%s: cannot open '%s' for writing %d %s\n",
                       __func__, file, errno, strerror(errno));
                return 0;
            }

            sb_out = cdb2buf_open(fd_out, 0);
            if (!sb_out) {
                Close(fd_out);
                return 0;
            }
        }
        {
            int version = INT_MAX;
            int lua_ver;
            int default_version =
                bdb_get_sp_get_default_version(new_sp, &bdberr);
            for (;;) {
                bdb_get_lua_highest(NULL, new_sp, &lua_ver, version, &bdberr);
                if (lua_ver < 1) break;
                char *lua_file;
                int size;
                if (bdb_get_sp_lua_source(NULL, NULL, new_sp, &lua_file,
                                          lua_ver, &size, &bdberr)) {
                    return 0;
                }
                struct sp_file_t *sp_f = malloc(sizeof(struct sp_file_t));
                strcpy(sp_f->name, new_sp);
                sp_f->default_version = 0;
                sp_f->version = lua_ver;
                sp_f->size = size;
                cdb2buf_write((char *)sp_f, sizeof(struct sp_file_t), sb_out);
                cdb2buf_write(lua_file, size, sb_out);
                version = lua_ver;
            }
            struct sp_file_t *sp_f = malloc(sizeof(struct sp_file_t));
            strcpy(sp_f->name, new_sp);
            sp_f->default_version = default_version;
            sp_f->version = 0;
            sp_f->size = 0;
            cdb2buf_write((char *)sp_f, sizeof(struct sp_file_t), sb_out);
        }
        strcpy(old_sp, new_sp);
    }
    cdb2buf_close(sb_out);
    return 1;
}

int read_spfile(const char *file)
{
    int bdberr;
    int fd_in = open(file, O_RDONLY);
    if (fd_in == -1) {
        return -1;
    }

    COMDB2BUF *sb_in = cdb2buf_open(fd_in, 0);
    if (!sb_in) {
        Close(fd_in);
        return -1;
    }

    while (1) {
        struct sp_file_t sp_f;
        if (sizeof(sp_f) != cdb2buf_fread((char *)&sp_f, 1, sizeof(sp_f), sb_in)) {
            cdb2buf_close(sb_in);
            return 0;
        }
        if (sp_f.size) {
            char *lua_file = malloc(sp_f.size);
            if (sp_f.size != cdb2buf_fread(lua_file, 1, sp_f.size, sb_in)) {
                free(lua_file);
                cdb2buf_close(sb_in);
                return -1;
            }

            if (bdb_set_sp_lua_source(NULL, NULL, sp_f.name, lua_file,
                                      sp_f.size, sp_f.version, &bdberr)) {
                free(lua_file);
                cdb2buf_close(sb_in);
                return -1;
            }

            free(lua_file);

        } else {
            if (bdb_set_sp_lua_default(NULL, NULL, sp_f.name,
                                       sp_f.default_version, &bdberr)) {
                cdb2buf_close(sb_in);
                return -1;
            }
        }
    }
    cdb2buf_close(sb_in);
    return 0;
}

/*
** REGULAR SPs: versioned by server specified monotonically increasing integer.
** VERSIONED SPs: versioned by client specified string.
**
** Going back and forth beween R5,6 may result in both defaults set (R5 is not
** aware of new scheme). Regular default always wins.
*/

// ------------------
// SHOW VERSIONED SPs
// ------------------
static void show_all_versioned_sps(struct schema_change_type *sc)
{
    COMDB2BUF *sb = sc->sb;
    char **names;
    int count;
    if (bdb_get_default_versioned_sps(&names, &count) != 0) return;
    for (int i = 0; i < count; ++i) {
        char *vstr;
        int vnum, bdberr;
        vnum = bdb_get_sp_get_default_version(names[i], &bdberr);
        if (vnum < 0 && bdb_get_default_versioned_sp(names[i], &vstr) == 0) {
            cdb2buf_printf(sb,
                        ">SP name: %s             Default Version is: '%s'\n",
                        names[i], vstr);
            free(vstr);
        }
        free(names[i]);
    }
    free(names);
}
static void show_versioned_sp(struct schema_change_type *sc, int *num)
{
    COMDB2BUF *sb = sc->sb;
    char *name = sc->tablename;
    char **versions = NULL;
    int rc;
    if ((rc = bdb_get_all_for_versioned_sp(name, &versions, num)) != 0)
        goto out;
    if (*num == 0) goto out;
    for (int i = 0; i < *num; ++i) {
        cdb2buf_printf(sb, ">Version: '%s'\n", versions[i]);
    }
out:
    for (int i = 0; i < *num; ++i) {
        free(versions[i]);
    }
    free(versions);
    return;
}
static int show_versioned_sp_src(struct schema_change_type *sc)
{
    COMDB2BUF *sb = sc->sb;
    char *name = sc->tablename;
    char *version = sc->newcsc2;
    char *src;
    if (bdb_get_versioned_sp(name, version, &src) != 0) {
        return -1;
    }
    char *sav = NULL, *line;
    line = strtok_r(src, "\n", &sav);
    while (line) {
        cdb2buf_printf(sb, ">%s\n", line);
        line = strtok_r(NULL, "\n", &sav);
    }
    free(src);
    return 0;
}
static int add_versioned_sp(struct schema_change_type *sc, tran_type *tran)
{
    int rc, bdberr;
    char *spname = sc->tablename;
    char *version = sc->fname;
    int default_ver_num = bdb_get_sp_get_default_version(spname, &bdberr);
    char *default_ver_str = NULL;
    bdb_get_default_versioned_sp(spname, &default_ver_str);
    free(default_ver_str);

    rc = bdb_add_versioned_sp(tran, spname, version, sc->newcsc2);
    if (default_ver_num <= 0 && default_ver_str == NULL) {
        // first version - set it default as well
        return bdb_set_default_versioned_sp(tran, spname, version);
    }
    return rc;
}
static int chk_versioned_sp_tran(char *name, char *version, struct ireq *iq, tran_type *tran)
{
    char *src = NULL;
    int rc = bdb_get_versioned_sp_tran(tran, name, version, &src);
    if (rc != 0) {
        if (iq) {
            errstat_cat_strf(&iq->errstat, "no such procedure %s:%s", name,
                             version);
        }
    }
    free(src);
    return rc;
}
static int chk_versioned_sp(char *name, char *version, struct ireq *iq)
{
    return chk_versioned_sp_tran(name, version, iq, NULL);
}
static int del_versioned_sp(struct schema_change_type *sc, struct ireq *iq)
{
    int rc;
    if ((rc = chk_versioned_sp(sc->tablename, sc->fname, iq)) != 0)
        return rc;
    return bdb_del_versioned_sp(sc->tablename, sc->fname);
}
static int default_versioned_sp(struct schema_change_type *sc, struct ireq *iq, tran_type *tran)
{
    int rc;
    if ((rc = chk_versioned_sp_tran(sc->tablename, sc->fname, iq, tran)) != 0)
        return rc;
    return bdb_set_default_versioned_sp(tran, sc->tablename, sc->fname);
}

// ----------------
// SHOW REGULAR SPs
// ----------------
static int show_all_sps(struct schema_change_type *sc)
{
    COMDB2BUF *sb = sc->sb;
    char old_sp[MAX_SPNAME];
    char new_sp[MAX_SPNAME];
    old_sp[0] = 127;
    while (1) {
        int bdberr;
        int rc = bdb_get_sp_name(NULL, old_sp, new_sp, &bdberr);
        if (rc || (strcmp(old_sp, new_sp) <= 0)) {
            cdb2buf_printf(sb, "SUCCESS\n");
            return 0;
        }
        char *vstr;
        int vnum = bdb_get_sp_get_default_version(new_sp, &bdberr);
        if (vnum >= 0) {
            cdb2buf_printf(sb, ">SP name: %s             Default Version is: %d\n",
                        new_sp, vnum);
        } else if (bdb_get_default_versioned_sp(new_sp, &vstr) == 0) {
            cdb2buf_printf(sb,
                        ">SP name: %s             Default Version is: '%s'\n",
                        new_sp, vstr);
            free(vstr);
        }
        strcpy(old_sp, new_sp);
    }
    cdb2buf_printf(sb, "SUCCESS\n");
    return 0;
}
static void show_sp(struct schema_change_type *sc, int *num, int *has_default)
{
    COMDB2BUF *sb = sc->sb;
    int version = INT_MAX;
    int lua_ver;
    int bdberr;
    *num = 0;
    *has_default = -1;
    for (;;) {
        bdb_get_lua_highest(NULL, sc->tablename, &lua_ver, version, &bdberr);
        if (lua_ver < 1) break;
        ++(*num);
        cdb2buf_printf(sb, ">Version: %d\n", lua_ver);
        version = lua_ver;
    }
    if (*num) {
        *has_default = bdb_get_sp_get_default_version(sc->tablename, &bdberr);
        if (*has_default > 0)
            cdb2buf_printf(sb, ">Default version is: %d\n", *has_default);
    }
}
static int show_sp_src(struct schema_change_type *sc)
{
    COMDB2BUF *sb = sc->sb;
    int version = atoi(sc->newcsc2);
    char *src;
    int size, bdberr;
    if (bdb_get_sp_lua_source(NULL, NULL, sc->tablename, &src, version, &size,
                              &bdberr)) {
        return -1;
    }
    char *sav = NULL, *line;
    line = strtok_r(src, "\n", &sav);
    while (line) {
        cdb2buf_printf(sb, ">%s\n", line);
        line = strtok_r(NULL, "\n", &sav);
    }
    free(src);
    return 0;
}

// ---------------------------
// ADD/DEL/DEFAULT REGULAR SPs
// ---------------------------
static int add_sp(struct schema_change_type *sc, int *version, tran_type *tran)
{
    COMDB2BUF *sb = sc->sb;
    char *schemabuf = sc->newcsc2;
    int rc, bdberr;
    if ((rc = bdb_set_sp_lua_source(NULL, tran, sc->tablename, schemabuf,
                                    strlen(schemabuf) + 1, 0, &bdberr)) != 0) {
        cdb2buf_printf(sb, "!Unable to add lua source. \n");
        cdb2buf_printf(sb, "FAILED\n");
        return rc;
    }
    bdb_get_lua_highest(tran, sc->tablename, version, INT_MAX, &bdberr);
    cdb2buf_printf(sb, "!Added stored procedure: %s \t version %d.\n",
                sc->tablename, *version);
    cdb2buf_printf(sb, "SUCCESS\n");
    return rc;
}
static int del_sp(struct schema_change_type *sc)
{
    COMDB2BUF *sb = sc->sb;
    if (sc->newcsc2 == NULL) {
        cdb2buf_printf(sb, "!missing sp version number\n");
        goto fail;
    }
    int version = atoi(sc->newcsc2);
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    int bdberr;
    if (bdb_delete_sp_lua_source(NULL, NULL, sc->tablename, version, &bdberr)) {
        cdb2buf_printf(sb, "!bad sp version number:%d\n", version);
        goto fail;
    }
    cdb2buf_printf(sb, "!Deleted stored procedure.\n");
    cdb2buf_printf(sb, "SUCCESS\n");
    return 0;
fail:
    cdb2buf_printf(sb, "FAILED\n");
    return -1;
}
static int default_sp(struct schema_change_type *sc, tran_type *tran)
{
    COMDB2BUF *sb = sc->sb;
    if (sc->newcsc2 == NULL) {
        cdb2buf_printf(sb, "!missing sp version number\n");
        goto fail;
    }
    int version = atoi(sc->newcsc2);
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    if (version == 0) {
        cdb2buf_printf(sb, "!bad sp version number\n");
        goto fail;
    }
    int bdberr;
    if (bdb_set_sp_lua_default(NULL, tran, sc->tablename, version, &bdberr)) {
        cdb2buf_printf(sb, "!version not found number %d\n", version);
        goto fail;
    }
    cdb2buf_printf(sb, "!Set the default procedure to version %d.\n", version);
    cdb2buf_printf(sb, "SUCCESS\n");
    return 0;
fail:
    cdb2buf_printf(sb, "FAILED\n");
    return -1;
}

// ---------------------------------------------------
// Decide whether to call regular or versioned or both
// ---------------------------------------------------
static int do_add_sp_int(struct schema_change_type *sc, struct ireq *iq, tran_type *tran)
{
    int rc, version;
    if (sc->fname[0] != 0) {
        rc = add_versioned_sp(sc, tran);
    } else {
        rc = add_sp(sc, &version, tran);
    }
    if (rc == 0) {
        ++gbl_lua_version;
        if (sc->kind != SC_DEFAULTCONS) {
            int bdberr;
            bdb_llog_luareload(thedb->bdb_env, 0, &bdberr);
        }
        if (iq) {
            if (sc->fname[0] == 0) {
                sprintf(sc->fname, "%d", version);
            }
            bzero(&iq->errstat, sizeof(iq->errstat));
            iq->errstat.errval = COMDB2_SCHEMACHANGE_OK;
            strcpy(iq->errstat.errstr, sc->fname);
        }
    }
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    return rc;
}
static int do_del_sp_int(struct schema_change_type *sc, struct ireq *iq)
{
    int rc;
    if (sc->fname[0] != 0) {
        rc = del_versioned_sp(sc, iq);
    } else {
        rc = del_sp(sc);
    }
    if (rc == 0) {
        int bdberr;
        bdb_llog_luareload(thedb->bdb_env, 0, &bdberr);
    }
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    return rc;
}
static int do_default_sp_int(struct schema_change_type *sc, struct ireq *iq, tran_type *tran)
{
    int rc;
    if (sc->fname[0] != 0) {
        rc = default_versioned_sp(sc, iq, tran);
    } else {
        rc = default_sp(sc, tran);
    }
    if (rc == 0 && sc->kind != SC_DEFAULTCONS) {
        int bdberr;
        bdb_llog_luareload(thedb->bdb_env, 0, &bdberr);
    }
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    return rc;
}

// ----------------
// PUBLIC INTERFACE
// ----------------
int do_show_sp(struct schema_change_type *sc, struct ireq *unused)
{
    COMDB2BUF *sb = sc->sb;
    if (sc->tablename[0] == 0) {
        show_all_versioned_sps(sc);
        show_all_sps(sc);
    } else if (sc->newcsc2[0] == 0) {
        int num0, num1, def;
        cdb2buf_printf(sb, ">Available versions are:\n");
        show_versioned_sp(sc, &num0);
        show_sp(sc, &num1, &def);
        if (num0 == 0 && num1 == 0) {
            cdb2buf_printf(sb, "!Stored procedure not found %s\n", sc->tablename);
            cdb2buf_printf(sb, "FAILED\n");
            return -1;
        }
        if (def == -1) {
            // check versioned default
            char *def_version;
            if (bdb_get_default_versioned_sp(sc->tablename, &def_version) ==
                0) {
                cdb2buf_printf(sb, ">Default version is: '%s'\n", def_version);
                free(def_version);
            } else {
                cdb2buf_printf(sb, ">Default version is: %d\n", def);
            }
        }
    } else {
        if (show_versioned_sp_src(sc) != 0 && show_sp_src(sc) != 0) {
            cdb2buf_printf(sb, "!version not found:%s\n", sc->newcsc2);
            cdb2buf_printf(sb, "FAILED\n");
            return -1;
        }
    }
    cdb2buf_printf(sb, "SUCCESS\n");
    return 0;
}

int do_add_sp_tran(struct schema_change_type *sc, struct ireq *iq, tran_type *tran, int lock_schema_lk)
{
    if (lock_schema_lk) { wrlock_schema_lk(); }
    int rc = do_add_sp_int(sc, iq, tran);
    ++gbl_lua_version;
    if (lock_schema_lk) { unlock_schema_lk(); }
    return !rc && !sc->finalize ? SC_COMMIT_PENDING : rc;
}

int do_add_sp(struct schema_change_type *sc, struct ireq *iq)
{
    return do_add_sp_tran(sc, iq, NULL, /* lock_schema_lk */ 1);
}

int finalize_add_sp(struct schema_change_type *sc)
{
    return 0;
}

int do_del_sp(struct schema_change_type *sc, struct ireq *iq)
{
    char *tbl = 0;
    if (lua_sfunc_used(sc->tablename, &tbl)) {
        char errmsg[64] = {0};
        sprintf(errmsg, "Can't drop. %s is in use by %s", sc->tablename, tbl);

        logmsg(LOGMSG_ERROR, "%s\n", errmsg);
        sc_errf(sc, "%s\n", errmsg);

        errstat_set_strf(&iq->errstat, "%s", errmsg);
        errstat_set_rc(&iq->errstat, SC_FUNC_IN_USE);

        return SC_FUNC_IN_USE;
    }
    wrlock_schema_lk();
    int rc = do_del_sp_int(sc, iq);
    ++gbl_lua_version;
    unlock_schema_lk();
    return !rc && !sc->finalize ? SC_COMMIT_PENDING : rc;
}

int finalize_del_sp(struct schema_change_type *sc)
{
    return 0;
}

int do_default_sp_tran(struct schema_change_type *sc, struct ireq *iq, tran_type *tran, int lock_schema_lk)
{
    if (lock_schema_lk) { wrlock_schema_lk(); }
    int rc = do_default_sp_int(sc, iq, tran);
    ++gbl_lua_version;
    if (lock_schema_lk) { unlock_schema_lk(); }
    return !rc && !sc->finalize ? SC_COMMIT_PENDING : rc;
}

int do_default_sp(struct schema_change_type *sc, struct ireq *iq)
{
    return do_default_sp_tran(sc, iq, NULL, /* lock_schema_lk */ 1);
}

int do_default_cons(struct schema_change_type *sc, struct ireq *iq)
{
    wrlock_schema_lk();
    javasp_splock_wrlock();
    
    tran_type *ltran = NULL;
    int rc = trans_start_logical_sc(iq, &ltran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: Failed to start logical transaction\n", __func__);
        goto done;
    }

    bdb_ltran_get_schema_lock(ltran);

    tran_type *tran = NULL;
    rc = get_physical_transaction(thedb->bdb_env, ltran, &tran, 0);
    if (rc != 0 || tran == NULL) {
        logmsg(LOGMSG_ERROR, "%s: Failed to start physical transaction\n", __func__);
        goto done;
    }

    rc = do_add_sp_tran(sc, iq, tran, /* lock_schema_lk */ 0);
    if (rc && rc != SC_COMMIT_PENDING) {
        logmsg(LOGMSG_ERROR, "%s: Failed to add procedure. rc %d\n", __func__, rc);
        goto done;
    }
    rc = do_default_sp_tran(sc, iq, tran, /* lock_schema_lk */ 0);
    if (rc && rc != SC_COMMIT_PENDING) {
        logmsg(LOGMSG_ERROR, "%s: Failed to make procedure default\n", __func__);
        goto done;
    }

    if (gbl_fail_to_create_default_cons) {
        rc = SC_ABORTED;
        goto done;
    }

    // This will commit/abort our transaction:
    rc = perform_trigger_update_tran(sc, iq, ltran, /* lock_schema_and_sp_lk */ 0);
    ltran = NULL;
    tran = NULL;
    if (rc && rc != SC_COMMIT_PENDING) {
        logmsg(LOGMSG_ERROR, "%s: Failed to create queue\n", __func__);
        goto done;
    }

done:
    if (ltran) { trans_abort(iq, ltran); }
    else if (tran) { trans_abort(iq, tran); }

    unlock_schema_lk();
    javasp_splock_unlock();

    return !rc && !sc->finalize ? SC_COMMIT_PENDING : rc;
}

int finalize_default_sp(struct schema_change_type *sc)
{
    return 0;
}

int finalize_default_cons(struct schema_change_type *sc)
{
    return 0;
}

// -----------------
// LUA SQL FUNCTIONS
// -----------------
#define reload_lua_funcs(type, pfx)                                            \
    BDB_BUMP_DBOPEN_GEN(type, NULL);                                           \
    ++gbl_lua_version;                                                         \
    do {                                                                       \
       lua_func_list_free(&thedb->lua_##pfx##funcs);                           \
       return llmeta_load_lua_##pfx##funcs();                                  \
    } while (0)

int reload_lua_sfuncs()
{
    reload_lua_funcs(lua_sfunc, s);
}

int reload_lua_afuncs()
{
    reload_lua_funcs(lua_afunc, a);
}

// clang-format off
#define finalize_lua_func(pfx)                                                 \
    do {                                                                       \
        int rc = reload_lua_##pfx##funcs();                                    \
        if (rc != 0) return rc;                                                \
        int bdberr;                                                            \
        return bdb_llog_luafunc(thedb->bdb_env, lua_##pfx##func, 0, &bdberr);  \
    } while (0)

// clang-format on
int finalize_lua_sfunc(struct schema_change_type *unused)
{
    finalize_lua_func(s);
}

int finalize_lua_afunc(struct schema_change_type *unused)
{
    finalize_lua_func(a);
}

#define do_lua_func(sc, rc, pfx)                                               \
    do {                                                                       \
        int bdberr;                                                            \
        if (sc->kind == SC_ADD_SFUNC || sc->kind == SC_ADD_AFUNC) {            \
            logmsg(LOGMSG_DEBUG, "%s -- adding lua sql func:%s\n", __func__,   \
                   sc->spname);                                                \
            bdb_llmeta_add_lua_##pfx##func(sc->spname, &sc->lua_func_flags,    \
                                           &bdberr);                           \
        } else {                                                               \
            logmsg(LOGMSG_DEBUG, "%s -- dropping lua sql func:%s\n", __func__, \
                   sc->spname);                                                \
            bdb_llmeta_del_lua_##pfx##func(sc->spname, &bdberr);               \
        }                                                                      \
        if (sc->finalize) {                                                    \
            rc = finalize_lua_##pfx##func(sc);                                 \
        } else                                                                 \
            rc = SC_COMMIT_PENDING;                                            \
    } while (0)

int do_lua_sfunc(struct schema_change_type *sc, struct ireq * iq)
{
    int rc = 0;
    char *tbl = 0;
    if (sc->kind != SC_ADD_SFUNC && (lua_sfunc_used(sc->spname, &tbl))) {
        char errmsg[64] = {0};
        sprintf(errmsg, "Can't drop. %s is in use by %s", sc->spname, tbl);

        logmsg(LOGMSG_ERROR, "%s\n", errmsg);
        sc_errf(sc, "%s\n", errmsg);

        errstat_set_strf(&iq->errstat, "%s", errmsg);
        errstat_set_rc(&iq->errstat, SC_FUNC_IN_USE);

        return SC_FUNC_IN_USE;
    }
    wrlock_schema_lk();
    do_lua_func(sc, rc, s);
    unlock_schema_lk();
    return rc;
}

int do_lua_afunc(struct schema_change_type *sc, struct ireq *unused)
{
    int rc;
    wrlock_schema_lk();
    do_lua_func(sc, rc, a);
    unlock_schema_lk();
    return rc;
}
