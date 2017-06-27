#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "schemachange.h"
#include "sc_lua.h"
#include "translistener.h"

#include "logmsg.h"

struct sp_file_t {
    char name[128]; // MAX_SPNAME
    int default_version;
    int version;
    int size;
};

int dump_spfile(char *path, const char *dbname, char *file_name)
{
    char old_sp[32];
    char new_sp[32];
    char file[1024];
    old_sp[0] = 127;
    int bdberr, rc = 0;
    int has_sp = 0;
    snprintf(file, sizeof(file), "%s/%s_%s", path, dbname, file_name);
    int fd_out = -1;
    SBUF2 *sb_out = NULL;

    while (rc == 0) {
        rc = bdb_get_sp_name(NULL, old_sp, new_sp, &bdberr);
        if (rc || (strcmp(old_sp, new_sp) <= 0)) {
            sbuf2close(sb_out);
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

            sb_out = sbuf2open(fd_out, 0);
            if (!sb_out) {
                close(fd_out);
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
                sbuf2write((char *)sp_f, sizeof(struct sp_file_t), sb_out);
                sbuf2write(lua_file, size, sb_out);
                version = lua_ver;
            }
            struct sp_file_t *sp_f = malloc(sizeof(struct sp_file_t));
            strcpy(sp_f->name, new_sp);
            sp_f->default_version = default_version;
            sp_f->version = 0;
            sp_f->size = 0;
            sbuf2write((char *)sp_f, sizeof(struct sp_file_t), sb_out);
        }
        strcpy(old_sp, new_sp);
    }
    sbuf2close(sb_out);
    return 1;
}

int read_spfile(char *file)
{
    int bdberr;
    int fd_in = open(file, O_RDONLY);
    if (fd_in == -1) {
        return -1;
    }

    SBUF2 *sb_in = sbuf2open(fd_in, 0);
    if (!sb_in) {
        close(fd_in);
        return -1;
    }

    while (1) {
        struct sp_file_t sp_f;
        if (sizeof(sp_f) != sbuf2fread((char *)&sp_f, 1, sizeof(sp_f), sb_in)) {
            sbuf2close(sb_in);
            return 0;
        }
        if (sp_f.size) {
            char *lua_file = malloc(sp_f.size);
            if (sp_f.size != sbuf2fread(lua_file, 1, sp_f.size, sb_in)) {
                free(lua_file);
                sbuf2close(sb_in);
                return -1;
            }

            if (bdb_set_sp_lua_source(NULL, NULL, sp_f.name, lua_file,
                                      sp_f.size, sp_f.version, &bdberr)) {
                free(lua_file);
                sbuf2close(sb_in);
                return -1;
            }

            free(lua_file);

        } else {
            if (bdb_set_sp_lua_default(NULL, NULL, sp_f.name,
                                       sp_f.default_version, &bdberr)) {
                sbuf2close(sb_in);
                return -1;
            }
        }
    }
    sbuf2close(sb_in);
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
    SBUF2 *sb = sc->sb;
    char **names;
    int count;
    if (bdb_get_default_versioned_sps(&names, &count) != 0) return;
    for (int i = 0; i < count; ++i) {
        char *vstr;
        int vnum, bdberr;
        vnum = bdb_get_sp_get_default_version(names[i], &bdberr);
        if (vnum < 0 && bdb_get_default_versioned_sp(names[i], &vstr) == 0) {
            sbuf2printf(sb,
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
    SBUF2 *sb = sc->sb;
    char *name = sc->table;
    char **versions = NULL;
    int rc;
    if ((rc = bdb_get_all_for_versioned_sp(name, &versions, num)) != 0)
        goto out;
    if (*num == 0) goto out;
    for (int i = 0; i < *num; ++i) {
        sbuf2printf(sb, ">Version: '%s'\n", versions[i]);
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
    SBUF2 *sb = sc->sb;
    char *name = sc->table;
    char *version = sc->newcsc2;
    char *src;
    if (bdb_get_versioned_sp(name, version, &src) != 0) {
        return -1;
    }
    char *sav, *line;
    line = strtok_r(src, "\n", &sav);
    while (line) {
        sbuf2printf(sb, ">%s\n", line);
        line = strtok_r(NULL, "\n", &sav);
    }
    free(src);
    return 0;
}
static int add_versioned_sp(struct schema_change_type *sc)
{
    return bdb_add_versioned_sp(sc->table, sc->fname, sc->newcsc2);
}
static int chk_versioned_sp(char *name, char *version, struct ireq *iq)
{
    char *src = NULL;
    int rc = bdb_get_versioned_sp(name, version, &src);
    if (rc != 0) {
        if (iq) {
            errstat_cat_strf(&iq->errstat, "no such procedure %s:%s", name,
                             version);
        }
    }
    free(src);
    return rc;
}
static int del_versioned_sp(struct schema_change_type *sc, struct ireq *iq)
{
    int rc;
    if ((rc = chk_versioned_sp(sc->table, sc->fname, iq)) != 0) return rc;
    return bdb_del_versioned_sp(sc->table, sc->fname);
}
static int default_versioned_sp(struct schema_change_type *sc, struct ireq *iq)
{
    int rc;
    if ((rc = chk_versioned_sp(sc->table, sc->fname, iq)) != 0) return rc;
    return bdb_set_default_versioned_sp(sc->table, sc->fname);
}

// ----------------
// SHOW REGULAR SPs
// ----------------
static int show_all_sps(struct schema_change_type *sc)
{
    SBUF2 *sb = sc->sb;
    char old_sp[MAX_SPNAME];
    char new_sp[MAX_SPNAME];
    old_sp[0] = 127;
    while (1) {
        int bdberr;
        int rc = bdb_get_sp_name(NULL, old_sp, new_sp, &bdberr);
        if (rc || (strcmp(old_sp, new_sp) <= 0)) {
            sbuf2printf(sb, "SUCCESS\n");
            return 0;
        }
        char *vstr;
        int vnum = bdb_get_sp_get_default_version(new_sp, &bdberr);
        if (vnum >= 0) {
            sbuf2printf(sb, ">SP name: %s             Default Version is: %d\n",
                        new_sp, vnum);
        } else if (bdb_get_default_versioned_sp(new_sp, &vstr) == 0) {
            sbuf2printf(sb,
                        ">SP name: %s             Default Version is: '%s'\n",
                        new_sp, vstr);
            free(vstr);
        }
        strcpy(old_sp, new_sp);
    }
    sbuf2printf(sb, "SUCCESS\n");
    return 0;
}
static void show_sp(struct schema_change_type *sc, int *num, int *has_default)
{
    SBUF2 *sb = sc->sb;
    int version = INT_MAX;
    int lua_ver;
    int bdberr;
    *num = 0;
    *has_default = -1;
    for (;;) {
        bdb_get_lua_highest(NULL, sc->table, &lua_ver, version, &bdberr);
        if (lua_ver < 1) break;
        ++(*num);
        sbuf2printf(sb, ">Version: %d\n", lua_ver);
        version = lua_ver;
    }
    if (*num) {
        *has_default = bdb_get_sp_get_default_version(sc->table, &bdberr);
        if (*has_default > 0)
            sbuf2printf(sb, ">Default version is: %d\n", *has_default);
    }
}
static int show_sp_src(struct schema_change_type *sc)
{
    SBUF2 *sb = sc->sb;
    int version = atoi(sc->newcsc2);
    char *src;
    int size, bdberr;
    if (bdb_get_sp_lua_source(NULL, NULL, sc->table, &src, version, &size,
                              &bdberr)) {
        return -1;
    }
    char *sav, *line;
    line = strtok_r(src, "\n", &sav);
    while (line) {
        sbuf2printf(sb, ">%s\n", line);
        line = strtok_r(NULL, "\n", &sav);
    }
    free(src);
    return 0;
}

// ---------------------------
// ADD/DEL/DEFAULT REGULAR SPs
// ---------------------------
static int add_sp(struct schema_change_type *sc, int *version)
{
    SBUF2 *sb = sc->sb;
    char *schemabuf = sc->newcsc2;
    int rc, bdberr;
    if ((rc = bdb_set_sp_lua_source(NULL, NULL, sc->table, schemabuf,
                                    strlen(schemabuf) + 1, 0, &bdberr)) != 0) {
        sbuf2printf(sb, "!Unable to add lua source. \n");
        sbuf2printf(sb, "FAILED\n");
        return rc;
    }
    bdb_get_lua_highest(NULL, sc->table, version, INT_MAX, &bdberr);
    sbuf2printf(sb, "!Added stored procedure: %s \t version %d.\n", sc->table,
                *version);
    sbuf2printf(sb, "SUCCESS\n");
    return rc;
}
static int del_sp(struct schema_change_type *sc)
{
    SBUF2 *sb = sc->sb;
    if (sc->newcsc2 == NULL) {
        sbuf2printf(sb, "!missing sp version number\n");
        goto fail;
    }
    int version = atoi(sc->newcsc2);
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    int bdberr;
    if (bdb_delete_sp_lua_source(NULL, NULL, sc->table, version, &bdberr)) {
        sbuf2printf(sb, "!bad sp version number:%d\n", version);
        goto fail;
    }
    sbuf2printf(sb, "!Deleted stored procedure.\n");
    sbuf2printf(sb, "SUCCESS\n");
    return 0;
fail:
    sbuf2printf(sb, "FAILED\n");
    return -1;
}
static int default_sp(struct schema_change_type *sc)
{
    SBUF2 *sb = sc->sb;
    if (sc->newcsc2 == NULL) {
        sbuf2printf(sb, "!missing sp version number\n");
        goto fail;
    }
    int version = atoi(sc->newcsc2);
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    if (version == 0) {
        sbuf2printf(sb, "!bad sp version number\n");
        goto fail;
    }
    int bdberr;
    if (bdb_set_sp_lua_default(NULL, NULL, sc->table, version, &bdberr)) {
        sbuf2printf(sb, "!version not found number %d\n", version);
        goto fail;
    }
    sbuf2printf(sb, "!Set the default procedure to version %d.\n", version);
    sbuf2printf(sb, "SUCCESS\n");
    return 0;
fail:
    sbuf2printf(sb, "FAILED\n");
    return -1;
}

// ---------------------------------------------------
// Decide whether to call regular or versioned or both
// ---------------------------------------------------
static int do_add_sp_int(struct schema_change_type *sc, struct ireq *iq)
{
    int rc, version;
    if (sc->fname[0] != 0) {
        rc = add_versioned_sp(sc);
    } else {
        rc = add_sp(sc, &version);
    }
    if (rc == 0) {
        ++gbl_lua_version;
        int bdberr;
        bdb_llog_luareload(thedb->bdb_env, 1, &bdberr);
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
        bdb_llog_luareload(thedb->bdb_env, 1, &bdberr);
    }
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    return rc;
}
static int do_default_sp_int(struct schema_change_type *sc, struct ireq *iq)
{
    int rc;
    if (sc->fname[0] != 0) {
        rc = default_versioned_sp(sc, iq);
    } else {
        rc = default_sp(sc);
    }
    if (rc == 0) {
        int bdberr;
        bdb_llog_luareload(thedb->bdb_env, 1, &bdberr);
    }
    free(sc->newcsc2);
    sc->newcsc2 = NULL;
    return rc;
}

// ----------------
// PUBLIC INTERFACE
// ----------------
int do_show_sp(struct schema_change_type *sc)
{
    SBUF2 *sb = sc->sb;
    if (sc->table[0] == 0) {
        show_all_versioned_sps(sc);
        show_all_sps(sc);
    } else if (sc->newcsc2[0] == 0) {
        int num0, num1, def;
        sbuf2printf(sb, ">Available versions are:\n");
        show_versioned_sp(sc, &num0);
        show_sp(sc, &num1, &def);
        if (num0 == 0 && num1 == 0) {
            sbuf2printf(sb, "!Stored procedure not found %s\n", sc->table);
            sbuf2printf(sb, "FAILED\n");
            return -1;
        }
        if (def == -1) {
            // check versioned default
            char *def_version;
            if (bdb_get_default_versioned_sp(sc->table, &def_version) == 0) {
                sbuf2printf(sb, ">Default version is: '%s'\n", def_version);
                free(def_version);
            } else {
                sbuf2printf(sb, ">Default version is: %d\n", def);
            }
        }
    } else {
        if (show_versioned_sp_src(sc) != 0 && show_sp_src(sc) != 0) {
            sbuf2printf(sb, "!version not found:%s\n", sc->newcsc2);
            sbuf2printf(sb, "FAILED\n");
            return -1;
        }
    }
    sbuf2printf(sb, "SUCCESS\n");
    return 0;
}
int do_add_sp(struct schema_change_type *sc, struct ireq *iq)
{
    wrlock_schema_lk();
    int rc = do_add_sp_int(sc, iq);
    ++gbl_lua_version;
    unlock_schema_lk();
    return rc;
}
int do_del_sp(struct schema_change_type *sc, struct ireq *iq)
{
    wrlock_schema_lk();
    int rc = do_del_sp_int(sc, iq);
    ++gbl_lua_version;
    unlock_schema_lk();
    return rc;
}
int do_default_sp(struct schema_change_type *sc, struct ireq *iq)
{
    wrlock_schema_lk();
    int rc = do_default_sp_int(sc, iq);
    ++gbl_lua_version;
    unlock_schema_lk();
    return rc;
}

// -----------------
// LUA SQL FUNCTIONS
// -----------------
#define reload_lua_funcs(pfx)                                                  \
    ++gbl_dbopen_gen;                                                          \
    ++gbl_lua_version;                                                         \
    do {                                                                       \
        for (int i = 0; i < thedb->num_lua_##pfx##funcs; ++i) {                \
            free(thedb->lua_##pfx##funcs[i]);                                  \
        }                                                                      \
        free(thedb->lua_##pfx##funcs);                                         \
        return llmeta_load_lua_##pfx##funcs();                                 \
    } while (0)

int reload_lua_sfuncs()
{
    reload_lua_funcs(s);
}

int reload_lua_afuncs()
{
    reload_lua_funcs(a);
}

#define finalize_lua_func(pfx)                                                 \
    do {                                                                       \
        int rc = reload_lua_##pfx##funcs();                                    \
        if (rc != 0) return rc;                                                \
        int bdberr;                                                            \
        return bdb_llog_luafunc(thedb->bdb_env, lua_##pfx##func, 1, &bdberr);  \
    } while (0)

int finalize_lua_sfunc()
{
    finalize_lua_func(s);
}

int finalize_lua_afunc()
{
    finalize_lua_func(a);
}

#define do_lua_func(sc, rc, pfx)                                               \
    do {                                                                       \
        int bdberr;                                                            \
        if (sc->addonly) {                                                     \
            logmsg(LOGMSG_DEBUG, "%s -- adding lua sql func:%s\n", __func__,   \
                   sc->spname);                                                \
            bdb_llmeta_add_lua_##pfx##func(sc->spname, &bdberr);               \
        } else {                                                               \
            logmsg(LOGMSG_DEBUG, "%s -- dropping lua sql func:%s\n", __func__, \
                   sc->spname);                                                \
            bdb_llmeta_del_lua_##pfx##func(sc->spname, &bdberr);               \
        }                                                                      \
        if (sc->finalize)                                                      \
            rc = finalize_lua_##pfx##func();                                   \
        else                                                                   \
            rc = SC_COMMIT_PENDING;                                            \
    } while (0)

int do_lua_sfunc(struct schema_change_type *sc)
{
    int rc;
    wrlock_schema_lk();
    do_lua_func(sc, rc, s);
    unlock_schema_lk();
    return rc;
}

int do_lua_afunc(struct schema_change_type *sc)
{
    int rc;
    wrlock_schema_lk();
    do_lua_func(sc, rc, a);
    unlock_schema_lk();
    return rc;
}
