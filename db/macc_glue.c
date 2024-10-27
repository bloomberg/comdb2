/*
   Copyright 2015, 2022, Bloomberg Finance L.P.

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

#include "macc_glue.h"
#include "tag.h"
#include "dynschematypes.h"
#include "dynschemaload.h"

static dbtable *newdb_from_schema(struct dbenv *env, char *tblname, int dbnum);
static int init_check_constraints(dbtable *tbl);
static int add_cmacc_stmt(dbtable *db, int alt, int allow_ull,
                          int no_side_effects, struct errstat *err);

char gbl_ver_temp_table[] = ".COMDB2.TEMP.VER.";

struct dbtable *create_new_dbtable(struct dbenv *dbenv, char *tablename,
                                   char *csc2, int dbnum, int sc_alt_tablename,
                                   int allow_ull, int no_side_effects,
                                   struct errstat *err)
{
    struct dbtable *newtable = NULL;
    int rc;

    dyns_init_globals();

    if (!tablename) {
        assert(no_side_effects);
        tablename = gbl_ver_temp_table;
    }

    rc = dyns_load_schema_string(csc2, dbenv->envname, tablename);
    if (rc) {
        char *syntax_error = csc2_get_syntax_errors();
        if (syntax_error) {
            errstat_set_rcstrf(err, -1, "%s", syntax_error);
        } else {
            errstat_set_rcstrf(err, -1, "dyns_load_schema_string failed for %s",
                               tablename);
        }
        goto err;
    }

    newtable = newdb_from_schema(dbenv, tablename, dbnum);
    if (!newtable) {
        errstat_set_rcstrf(err, -1, "newdb_from_schema failed for %s",
                           tablename);
        rc = -1;
        goto err;
    }

    newtable->csc2_schema = strdup(csc2);
    newtable->csc2_schema_len = strlen(csc2);

    rc = add_cmacc_stmt(newtable, sc_alt_tablename, allow_ull, no_side_effects,
                        err);
    if (rc) {
        goto err;
    }

    if (newtable->schema) {
        rc = init_check_constraints(newtable);
        if (rc) {
            errstat_set_rcstrf(err, -1,
                               "Failed to load check constraints "
                               "for %s",
                               newtable->tablename);
            goto err;
        }
    }

err:
    if (rc) {
        if (newtable) {
            cleanup_newdb(newtable);
            newtable = NULL;
        }
    }

    dyns_cleanup_globals();
    return newtable;
}

int populate_db_with_alt_schema(struct dbenv *dbenv, struct dbtable *db,
                                char *csc2, struct errstat *err)
{
    int rc;

    dyns_init_globals();

    rc = dyns_load_schema_string(csc2, dbenv->envname, db->tablename);
    if (rc) {
        errstat_set_rcstrf(err, -1, "%s failed to load alternate schema rc %d",
                           __func__, rc);
        goto done;
    }

    /* create the .NEW. tags, but don't update db->lrl, numix, numblobs
     * or anything like that. */
    rc = add_cmacc_stmt(db, 1 /* altname */, 0 /* no ull*/,
                        1 /* no side effects */, err);
    if (rc) {
        goto done;
    }

done:
    dyns_cleanup_globals();
    return rc;
}

static dbtable *newdb_from_schema(struct dbenv *env, char *tblname, int dbnum)
{
    dbtable *tbl;
    int ii;
    int tmpidxsz;
    int rc;

    tbl = calloc(1, sizeof(dbtable));
    if (tbl == NULL) {
        logmsg(LOGMSG_FATAL, "%s: Memory allocation error\n", __func__);
        return NULL;
    }

    tbl->dbtype = DBTYPE_TAGGED_TABLE;
    tbl->tablename = strdup(tblname);
    tbl->dbenv = env;
    tbl->dbnum = dbnum;
    tbl->lrl = dyns_get_db_table_size(); /* this gets adjusted later */
    Pthread_rwlock_init(&tbl->sc_live_lk, NULL);
    Pthread_mutex_init(&tbl->rev_constraints_lk, NULL);
    if (dbnum == 0) {
        /* if no dbnumber then no default tag is required ergo lrl can be 0 */
        if (tbl->lrl < 0)
            tbl->lrl = 0;
        else if (tbl->lrl > MAXLRL) {
            logmsg(LOGMSG_ERROR, "bad data lrl %d in csc schema %s\n", tbl->lrl,
                   tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
    } else {
        /* this table must have a default tag */
        int ntags, itag;
        ntags = dyns_get_table_count();
        for (itag = 0; itag < ntags; itag++) {
            if (strcasecmp(dyns_get_table_tag(itag), ".DEFAULT") == 0)
                break;
        }
        if (ntags == itag) {
            logmsg(LOGMSG_ERROR,
                   "csc schema %s requires comdbg compatibility but "
                   "has no default tag\n",
                   tblname);
            cleanup_newdb(tbl);
            return NULL;
        }

        if (tbl->lrl < 1 || tbl->lrl > MAXLRL) {
            logmsg(LOGMSG_ERROR, "bad data lrl %d in csc schema %s\n", tbl->lrl,
                   tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
    }
    tbl->nix = dyns_get_idx_count();
    if (tbl->nix > MAXINDEX) {
        logmsg(LOGMSG_ERROR, "too many indices %d in csc schema %s\n", tbl->nix,
               tblname);
        cleanup_newdb(tbl);
        return NULL;
    }
    for (ii = 0; ii < tbl->nix; ii++) {
        tmpidxsz = dyns_get_idx_size(ii);
        if (tmpidxsz < 1 || tmpidxsz > MAXKEYLEN) {
            logmsg(LOGMSG_ERROR, "index %d bad keylen %d in csc schema %s\n",
                   ii, tmpidxsz, tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
        tbl->ix_keylen[ii] = tmpidxsz; /* ix lengths are adjusted later */

        tbl->ix_dupes[ii] = dyns_is_idx_dup(ii);
        if (tbl->ix_dupes[ii] < 0) {
            logmsg(LOGMSG_ERROR, "cant find index %d dupes in csc schema %s\n",
                   ii, tblname);
            cleanup_newdb(tbl);
            return NULL;
        }

        tbl->ix_recnums[ii] = dyns_is_idx_recnum(ii);
        if (tbl->ix_recnums[ii] < 0) {
            logmsg(LOGMSG_ERROR,
                   "cant find index %d recnums in csc schema %s\n", ii,
                   tblname);
            cleanup_newdb(tbl);
            return NULL;
        }

        tbl->ix_datacopy[ii] = dyns_is_idx_datacopy(ii) | dyns_is_idx_partial_datacopy(ii);
        if (tbl->ix_datacopy[ii] < 0) {
            logmsg(LOGMSG_ERROR,
                   "cant find index %d datacopy in csc schema %s\n", ii,
                   tblname);
            cleanup_newdb(tbl);
            return NULL;
        } else if (tbl->ix_datacopy[ii]) {
            tbl->has_datacopy_ix = 1;
        }

        tbl->ix_nullsallowed[ii] = dyns_is_idx_uniqnulls(ii);
        if (tbl->ix_nullsallowed[ii] < 0) {
            logmsg(LOGMSG_ERROR,
                   "cant find index %d uniqnulls in csc schema %s\n", ii,
                   tblname);
            cleanup_newdb(tbl);
            return NULL;
        }
    }

    init_reverse_constraints(tbl);

    tbl->n_constraints = dyns_get_constraint_count();
    if (tbl->n_constraints > 0) {
        char *consname = NULL;
        char *keyname = NULL;
        int rulecnt = 0, flags = 0;
        tbl->constraints = calloc(tbl->n_constraints, sizeof(constraint_t));
        for (ii = 0; ii < tbl->n_constraints; ii++) {
            rc = dyns_get_constraint_at(ii, &consname, &keyname, &rulecnt,
                                        &flags);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "Cannot get constraint at %d (cnt=%zu)!\n",
                       ii, tbl->n_constraints);
                cleanup_newdb(tbl);
                return NULL;
            }
            tbl->constraints[ii].flags = flags;
            tbl->constraints[ii].lcltable = tbl;
            tbl->constraints[ii].consname = consname ? strdup(consname) : 0;
            tbl->constraints[ii].lclkeyname = (keyname) ? strdup(keyname) : 0;
            tbl->constraints[ii].nrules = rulecnt;

            tbl->constraints[ii].table = calloc(tbl->constraints[ii].nrules,
                                                MAXTABLELEN * sizeof(char *));
            tbl->constraints[ii].keynm =
                calloc(tbl->constraints[ii].nrules, MAXKEYLEN * sizeof(char *));

            if (tbl->constraints[ii].nrules > 0) {
                int jj = 0;
                for (jj = 0; jj < tbl->constraints[ii].nrules; jj++) {
                    char *tblnm = NULL;
                    rc = dyns_get_constraint_rule(ii, jj, &tblnm, &keyname);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR,
                               "cannot get constraint rule %d table %s:%s\n",
                               jj, tblname, keyname);
                        cleanup_newdb(tbl);
                        return NULL;
                    }
                    tbl->constraints[ii].table[jj] = strdup(tblnm);
                    tbl->constraints[ii].keynm[jj] = strdup(keyname);
                }
            }
        } /* for (ii...) */
    }     /* if (n_constraints > 0) */
    tbl->ixuse = calloc(tbl->nix, sizeof(unsigned long long));
    tbl->sqlixuse = calloc(tbl->nix, sizeof(unsigned long long));
    return tbl;
}

static int _clone_column(struct field *src, struct field *m, int offset)
{
    m->name = strdup(src->name);
    m->flags = src->flags;
    m->blob_index = src->blob_index;
    m->type = src->type;
    m->len = src->len;
    memcpy(&m->convopts, &src->convopts, sizeof(struct field_conv_opts));
    m->offset = offset;
    return offset + m->len;
}

static struct schema * _create_index_datacopy_schema(struct schema *sch, int ix,
                                                     struct errstat *err)
{
    struct partial_datacopy *pd;
    struct schema *p = NULL;
    int numMembers = 0;
    int rc;

    rc = dyns_get_idx_partial_datacopy(ix, &pd);
    if (rc || !pd) {
        errstat_set_rcstrf(err, rc = -1,
                "cannot form partial datacopy for index %d.", ix);
        goto err;
    }

    // find number of members
    struct partial_datacopy *temp = pd;
    while (temp) {
        temp = temp->next;
        numMembers++;
    }

    char *tmp = strdup("PARTIAL_DATACOPY");
    if (tmp)
        p = alloc_schema(tmp, numMembers,
                         SCHEMA_PARTIALDATACOPY_ACTUAL);
    if (!p) {
        errstat_set_rcstrf(err, rc = -1,
                "oom %s:%d index %d", __func__, __LINE__, ix);
        goto err;
    }

    temp = pd;
    struct field *m;
    int piece = 0;
    int offset = 0;
    while (temp) {
        m = &p->member[piece];
        m->idx = find_field_idx_in_tag(sch, temp->field);
        if (m->idx == -1) {
            /* DEFAULT tag can fail here, but error is ignored, 
             * no datacopy idx for comdbgi
             */
            rc = -ix - 1;
            goto err;
        }

        offset = _clone_column(&sch->member[m->idx], m, offset);

        temp = temp->next;
        piece++;
    }

err:
    if (rc) {
        if (p) {
            free(p->tag);
            free(p->member);
            free(p);
            p = NULL;
        }
    }
    return p;
}

static struct schema *_create_index_schema(const char *tag, int ix,
                                           struct errstat *err)
{
    struct schema *s = NULL;
    char buf[MAXCOLNAME + 1];


    snprintf(buf, sizeof(buf), "%s_ix_%d", tag, ix);

    char *tmp = strdup(buf);
    if (tmp)
        s = alloc_schema(tmp, dyns_get_idx_piece_count(ix),
                         SCHEMA_INDEX);
    if (!s) {
        errstat_set_rcstrf(err, -1, "oom: %s:%d", __func__, __LINE__);
        return NULL;
    }

    if (dyns_is_idx_dup(ix))
        s->flags |= SCHEMA_DUP;

    if (dyns_is_idx_recnum(ix))
        s->flags |= SCHEMA_RECNUM;

    if (dyns_is_idx_datacopy(ix))
        s->flags |= SCHEMA_DATACOPY;

    if (dyns_is_idx_uniqnulls(ix))
        s->flags |= SCHEMA_UNIQNULLS;

    s->ixnum = ix;

    return s;
}

char *indexes_expressions_unescape(char *expr);

static int _set_schema_index_column(struct dbtable *tbl, struct schema *sch, struct field *m,
                                    int ix, int piece, int offset,
                                    struct errstat *err)
{
    char buf[MAXCOLNAME + 1];
    char *expr = NULL;
    int ascdesc;

    dyns_get_idx_piece(ix, piece, buf, sizeof(buf), &m->type,
                       (int *)&m->offset, (int *)&m->len, &ascdesc,
                       &expr);
    if (ascdesc)
        m->flags |= INDEX_DESCEND;
    if (expr) {
        expr = indexes_expressions_unescape(expr);
        m->name = expr;
        m->isExpr = 1;
        m->idx = -1;
        if (expr == NULL) {
            errstat_set_rcstrf(err, -1, "unterminated string literal");
            return 1;
        }
        switch (m->type) {
            case 0:
                abort();
            case SERVER_BLOB:
            case SERVER_VUTF8:
            case SERVER_BLOB2:
                errstat_set_rcstrf(err, -1, "blob index is not supported.");
                return 1;
            case SERVER_BCSTR:
                if (m->len < 2) {
                    errstat_set_rcstrf(
                            err, -1,
                            "string must be at least 2 bytes in in length.");
                    return 1;
                }
                break;
            case SERVER_DATETIME:
                /* server CLIENT_DATETIME is a server_datetime_t */
                m->len = sizeof(server_datetime_t);
                break;
            case SERVER_DATETIMEUS:
                /* server CLIENT_DATETIMEUS is a server_datetimeus_t */
                m->len = sizeof(server_datetimeus_t);
                break;
            case SERVER_INTVYM:
                /* server CLIENT_INTVYM is a server_intv_ym_t */
                m->len = sizeof(server_intv_ym_t);
                break;
            case SERVER_INTVDS:
                /* server CLIENT_INTVDS is a server_intv_ds_t */
                m->len = sizeof(server_intv_ds_t);
                break;
            case SERVER_INTVDSUS:
                /* server CLIENT_INTVDSUS is a server_intv_dsus_t */
                m->len = sizeof(server_intv_dsus_t);
                break;
            case SERVER_DECIMAL:
                switch (m->len) {
                    case 14:
                        m->len = sizeof(server_decimal32_t);
                        break;
                    case 24:
                        m->len = sizeof(server_decimal64_t);
                        break;
                    case 43:
                        m->len = sizeof(server_decimal128_t);
                        break;
                    default:
                        abort();
                }
                break;
            default:
                /* other types just add one for the flag byte */
                m->len++;
                break;
        }
        if (offset + m->len > MAXKEYLEN) {
            errstat_set_rcstrf(err, -1, "index %d is too large.", ix);
            return 1;
        }
    } else {
        m->name = strdup(buf);
        m->idx = find_field_idx_in_tag(sch, m->name);
        if (m->idx == -1) {
            errstat_set_rcstrf(err, -1, "field %s not found in %s",
                    m->name, sch->tag);
            return -ix - 1; /* this is ignored for DEFAULT tag */
        }
        m->type = sch->member[m->idx].type;
        m->len = sch->member[m->idx].len;
        if (tbl->periods[PERIOD_SYSTEM].enable) {
            if (m->idx == tbl->periods[PERIOD_SYSTEM].start)
                m->flags |= SYSTEM_START;
            else if (m->idx == tbl->periods[PERIOD_SYSTEM].end)
                m->flags |= SYSTEM_END;
        }

        /* the dbstore default is still needed during schema change, so
         * populate that */
        if (sch->member[m->idx].in_default) {
            m->in_default = malloc(sch->member[m->idx].in_default_len);
            if (!m->in_default) {
                errstat_set_rcstrf(err, -1, "oom %s:%d", __func__, __LINE__);
                return -ix - 1;
            }
            m->in_default_len = sch->member[m->idx].in_default_len;
            m->in_default_type = sch->member[m->idx].in_default_type;
            memcpy(m->in_default, sch->member[m->idx].in_default,
                   m->in_default_len);
        }
        memcpy(&m->convopts, &sch->member[m->idx].convopts,
                sizeof(struct field_conv_opts));
    }
    m->offset = offset;
    return 0;
}


extern int gbl_new_indexes;

/* create keys for each schema */
static int create_keys_schemas(dbtable *tbl, struct schema *sch, int alt,
                               int is_ondisk, struct schema **lst, int *pnlst,
                               struct errstat *err)
{
    int ix;
    int piece;
    int offset;
    char altname[MAXTAGLEN];
    char *where;
    int rc;
    char *tblname = tbl->tablename;
    struct schema *s;
    int backup = *pnlst;

    sch->nix = tbl->nix;
    sch->ix = calloc(sch->nix, sizeof(struct schema *));
    if (!sch->ix) {
        errstat_set_rcstrf(err, rc = -1, "oom: %s:%d", __func__, __LINE__);
        goto err;
    }

    for (ix = 0; ix < sch->nix; ix++) {
        sch->ix[ix] = s = _create_index_schema(sch->tag, ix, err);
        if (!s) {
            rc = -1;
            goto err;
        }

        if (dyns_is_idx_partial_datacopy(ix)) {
            s->flags |= SCHEMA_PARTIALDATACOPY;
            s->partial_datacopy = _create_index_datacopy_schema(sch, ix, err);
            if (!s->partial_datacopy) {
                rc = -1;
                goto err;
            }
            tbl->ix_datacopylen[ix] =
                s->partial_datacopy->member[s->partial_datacopy->nmembers-1].offset +
                s->partial_datacopy->member[s->partial_datacopy->nmembers-1].len;
        } else
            tbl->ix_datacopylen[ix] = 0;

        if (is_ondisk)
            tbl->ixschema[ix] = sch->ix[ix];

        offset = 0;
        for (piece = 0; piece < s->nmembers; piece++) {
            if ((rc = _set_schema_index_column(tbl, sch, &s->member[piece], ix, piece, offset, err)))
                goto err;
            offset += s->member[piece].len;

            /* we could check here if there are decimals in the index, and mark
             * it "datacopy" special */
            if (tbl->ix_datacopy[ix] == 0 && s->member[piece].type == SERVER_DECIMAL) {
                tbl->ix_collattr[ix]++; /* special tail */
            } else if (s->member[piece].type == SERVER_DECIMAL) {
                tbl->ix_datacopy[ix]++; /* special tail */
            }
            if (!tbl->ix_expr) {
                if (gbl_new_indexes &&
                        strncasecmp(tblname, "sqlite_stat", 11) &&
                        strcasecmp(tblname, "comdb2_oplog") &&
                        strcasecmp(tblname, "comdb2_commit_log") &&
                        (!gbl_replicate_local ||
                         strcasecmp(s->member[piece].name, "comdb2_seqno"))) {
                    s->member[piece].isExpr = 1;
                    tbl->ix_partial = 1;
                }
                tbl->ix_expr = s->member[piece].isExpr;
            }
        }
        tbl->ix_keylen[ix] = offset;

        lst[(*pnlst)++] = s;

        rc = dyns_get_idx_tag(ix, altname, MAXTAGLEN, &where);
        if (!rc && is_ondisk) {
            struct schema *s_alias;
            if (alt == 0) {
                s_alias = clone_schema_index(s, altname, sch->nmembers);
                if (!s_alias)  {
                    errstat_set_rcstrf(err, rc = -1, "Failed to clone tag %s",
                                       s_alias->tag);
                    return 1;
                }
            } else {
                char tmptagname[MAXTAGLEN + sizeof(".NEW.")];
                snprintf(tmptagname, sizeof(tmptagname), ".NEW.%s", altname);
                s_alias = clone_schema_index(s, tmptagname, sch->nmembers);
                if (!s_alias)  {
                    errstat_set_rcstrf(err, rc = -1, "Failed to clone tag %s",
                                       s_alias->tag);
                    return 1;
                }
            }
            if (!s->csctag) {
                s->csctag = strdup(s_alias->tag);
                if (!s->csctag)
                    return 1;
            }
            lst[(*pnlst)++] = s_alias;
            if (where) {
                s->where = strdup(where);
                if (!s->where)
                    return 1;
                tbl->ix_partial = 1;
            }
        }
    }
    return 0;

err:
    for (ix = backup; ix < *pnlst; ix++) {
        freeschema(lst[ix], 0);
        lst[ix] = NULL;
    }
    *pnlst = backup;
    free(sch->ix);
    sch->ix = NULL;
    sch->nix = 0;
    return rc;
}

/* Setup the dbload/dbstore values for a field.  Only do this for the
 * ondisk tag.  The in/out default values are stored in server format
 * but with the same type as the dbload/dbstore value uses in the csc2
 * (why? - why not just convert it here so later on we can memcpy?)
 */
static int init_default_value(struct field *fld, int fldn, int loadstore)
{
    const char *name;
    void **p_default;
    int *p_default_len;
    int *p_default_type;
    int mastersz;
    void *typebuf = NULL;
    int opttype = 0;
    int optsz;
    int rc, outrc = 0;

    switch (loadstore) {
    case FLDOPT_DBSTORE:
        name = "dbstore";
        p_default = &fld->in_default;
        p_default_len = &fld->in_default_len;
        p_default_type = &fld->in_default_type;
        break;

    case FLDOPT_DBLOAD:
        name = "dbload";
        p_default = &fld->out_default;
        p_default_len = &fld->out_default_len;
        p_default_type = &fld->out_default_type;
        break;

    default:
        logmsg(LOGMSG_FATAL, "init_default_value: loadstore=%d?!\n", loadstore);
        exit(1);
    }

    if (fld->type == SERVER_VUTF8) {
        mastersz = 16 * 1024;
    } else if (fld->type == SERVER_DATETIME || fld->type == SERVER_DATETIMEUS) {
        mastersz =
            CLIENT_DATETIME_EXT_LEN; /* We want to get back cstring here. */
    } else {
        mastersz = max_type_size(fld->type, fld->len);
    }

    if (mastersz > 0)
        typebuf = calloc(1, mastersz);

    char *func_str = NULL;
    rc = dyns_get_table_field_option(".ONDISK", fldn, loadstore, &opttype,
                                     &optsz, typebuf, mastersz, &func_str);

    *p_default = NULL;
    *p_default_len = 0;

    if (rc == -1 && opttype != CLIENT_MINTYPE) {
        /* csc2 doesn't like its default value data, or our buffer is too small
         */
        logmsg(LOGMSG_ERROR, "init_default_value: %s for %s is invalid\n", name,
               fld->name);
        outrc = -1;
    } else if (rc == 0) {
        int outdtsz;
        int is_null = 0;

        /* if we are dealing with a default for vutf8 we must be sure to save it
         * a
         * as a cstring, we don't want to have to hold on to a "default blob" */
        if (opttype == CLIENT_VUTF8) {
            opttype = CLIENT_CSTR;
            *p_default_len = optsz + 1 /* csc2lib doesn't count NUL byte*/;
        } else
            *p_default_len = fld->len;

        *p_default_type = client_type_to_server_type(opttype);

        if (opttype == CLIENT_FUNCTION) {
            *p_default = strdup(func_str);
            outrc = 0;
            goto out;
        }

        *p_default = calloc(1, *p_default_len);

        if (opttype == CLIENT_DATETIME || opttype == CLIENT_DATETIMEUS) {
            opttype = CLIENT_CSTR;
            if (strncasecmp(typebuf, "CURRENT_TIMESTAMP", 17) == 0)
                is_null = 1; /* use isnull flag for current timestamp since
                                null=yes is used for dbstore null */
        }

        if (*p_default == NULL) {
            logmsg(LOGMSG_ERROR, "init_default_value: out of memory\n");
            outrc = -1;
        } else {
            /* csc2lib doesn't count the \0 in the size for cstrings - but type
             * system does and will balk if no \0 is found. */
            if (opttype == CLIENT_CSTR)
                optsz++;

            if (opttype == CLIENT_SEQUENCE && loadstore == FLDOPT_DBSTORE) {
                set_resolve_master(*p_default, *p_default_len);
                rc = 0;
            } else {
                rc = CLIENT_to_SERVER(typebuf, optsz, opttype,
                                      is_null /*isnull*/, NULL /*convopts*/,
                                      NULL /*blob*/, *p_default, *p_default_len,
                                      *p_default_type, 0, &outdtsz,
                                      &fld->convopts, NULL /*blob*/);
            }
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, "%s initialisation failed for field %s\n",
                       name, fld->name);
                free(*p_default);
                *p_default = NULL;
                *p_default_len = 0;
                *p_default_type = 0;
                outrc = -1;
            }
        }
    }

out:
    if (typebuf)
        free(typebuf);

    return outrc;
}

static struct schema * _create_table_schema(char *tag, int alt)
{
    struct schema *sch = NULL;
    char *ondisk_tag;

    if (alt == 0) {
        ondisk_tag = strdup(tag);
    } else {
        char tmptagname[MAXTAGLEN] = {0};
        snprintf(tmptagname, sizeof(tmptagname), ".NEW.%s", tag);
        ondisk_tag = strdup(tmptagname);
    }

    if (ondisk_tag)
        sch = alloc_schema(ondisk_tag, dyns_get_table_field_count(tag),
                           SCHEMA_TABLE);
    if (!sch)
        return NULL;

    /* ondisk format has an extra byte per field, and some other types have other
     * way to store (strings, blobs); this is why ondisk is wrong here, since 
     * parser just returns the length of the values!
     */
    sch->recsize = dyns_get_table_tag_size(tag); /* wrong for .ONDISK */

    return sch;
}

static int _check_column_ull(struct field *fld, const char *tblname, int allow_ull)
{
    extern int gbl_forbid_ulonglong;
    if (gbl_forbid_ulonglong &&
            ((fld->type == SERVER_UINT && fld->len == (sizeof(unsigned long long) + 1)) ||
            (fld->type == CLIENT_UINT && fld->len == sizeof(unsigned long long)))) {
        logmsg(LOGMSG_ERROR,
                "Error in table %s: u_longlong is unsupported\n",
                tblname);
        /* Skip returning error on presence of u_longlong if:

           - we haven't fully started, or
           - we were explicitly asked (by the callers) to do so

           This is done to allow existing databases with tables
           containing u_longlong to start and also certain operational
           commands, like table rebuilds, truncates and time partition
           rollouts, to succeed.
           */
        if (gbl_ready && !allow_ull) {
            return -1;
        }
    }

    return 0;
}

/* returns number of blobs */
static int _set_column_blobs(struct field *fld, int nblobs)
{
    /* count the blobs */
    if (fld->type == CLIENT_BLOB ||
        fld->type == CLIENT_VUTF8 ||
        fld->type == CLIENT_BLOB2 ||
        fld->type == SERVER_BLOB ||
        fld->type == SERVER_BLOB2 ||
        fld->type == SERVER_VUTF8) {
        fld->blob_index = nblobs;
        nblobs++;
    } else {
        fld->blob_index = -1;
    }

    return nblobs;
}

/* for ondisk, we need to set the field length to server type proper
 * return current offset
 */
static int _set_column_ondisk_length(struct field *fld, int idx, char *tag,
                                    int offset)
{
    char buf[MAXCOLNAME + 1] = {0}; /* scratch space buffer */

    /* cheat: change type to be ondisk type */
    switch (fld->type) {
        case SERVER_BLOB:
            /* TODO use the enums that are used in types.c */
            /* blobs are stored as flag byte + 32 bit length */
            fld->len = 5;
            break;
        case SERVER_VUTF8:
        case SERVER_BLOB2:
            /* TODO use the enums that are used in types.c */
            /* vutf8s are stored as flag byte + 32 bit length, plus
             * optionally strings up to a certain length can be stored
             * in the record itself */
            if (fld->len == -1)
                fld->len = 5;
            else
                fld->len += 5;
            break;
        case SERVER_BCSTR: {
            int clnt_type = 0;
            int clnt_offset = 0;
            int clnt_len = 0;

            dyns_get_table_field_info(tag, idx, buf, sizeof(buf),
                                      &clnt_type, &clnt_offset, NULL,
                                      &clnt_len, NULL, 0);

            if (clnt_type == CLIENT_PSTR || clnt_type == CLIENT_PSTR2) {
                fld->len++;
            } else {
                /* no change needed from client length */
            }
            } break;
        case SERVER_DATETIME:
            /* server CLIENT_DATETIME is a server_datetime_t */
            fld->len = sizeof(server_datetime_t);
            break;
        case SERVER_DATETIMEUS:
            /* server CLIENT_DATETIMEUS is a server_datetimeus_t */
            fld->len = sizeof(server_datetimeus_t);
            break;
        case SERVER_INTVYM:
            /* server CLIENT_INTVYM is a server_intv_ym_t */
            fld->len = sizeof(server_intv_ym_t);
            break;
        case SERVER_INTVDS:
            /* server CLIENT_INTVDS is a server_intv_ds_t */
            fld->len = sizeof(server_intv_ds_t);
            break;
        case SERVER_INTVDSUS:
            /* server CLIENT_INTVDSUS is a server_intv_dsus_t */
            fld->len = sizeof(server_intv_dsus_t);
            break;
        case SERVER_DECIMAL:
            switch (fld->len) {
            case 14:
                fld->len = sizeof(server_decimal32_t);
                break;
            case 24:
                fld->len = sizeof(server_decimal64_t);
                break;
            case 43:
                fld->len = sizeof(server_decimal128_t);
                break;
            default:
                abort();
            }
            break;
        default:
            /* other types just add one for the flag byte */
            fld->len++;
            break;
    }
    fld->offset = offset;
    offset += fld->len;
    return offset;
}

static int _set_column_options(struct field *fld, int idx, char *tag,
                               struct errstat *err)
{
    int type, sz, isnull, padval;
    int rc;

    /* field allowed to be null? */
    rc = dyns_get_table_field_option(tag, idx, FLDOPT_NULL,
                                     &type, &sz, &isnull,
                                     sizeof(int), NULL);
    if (rc == 0 && isnull) {
        fld->flags &= ~NO_NULL;
    } else {
        /* no null option, not an error even thought rc = -1 */
    }

    /* dbpad value (used for byte array conversions).  If it is
     * present then type must be integer.
     */
    rc = dyns_get_table_field_option(tag, idx, FLDOPT_PADDING,
                                     &type, &sz, &padval,
                                     sizeof(padval), NULL);
    if (rc == 0 && CLIENT_INT == type) {
        fld->convopts.flags |= FLD_CONV_DBPAD;
        fld->convopts.dbpad = padval;
    } else {
        /* no padding option, not an error even thought rc = -1 */
    }

    /* input default */
    rc = init_default_value(fld, idx, FLDOPT_DBSTORE);
    if (rc != 0) {
        errstat_set_rcstrf(err, -1, "invalid default column value");
        return -1;
    }

    /* output default  */
    rc = init_default_value(fld, idx, FLDOPT_DBLOAD);
    if (rc != 0) {
        errstat_set_rcstrf(err, -1, "invalid dbpad value");
        return -1;
    }

    return 0;
}

static struct field *_create_schema_column(struct field *fld, int idx,
                                           char *tag, int is_ondisk_schema,
                                           struct errstat *err, int *offset)
{
    char buf[MAXCOLNAME + 1] = {0}; /* scratch space buffer */

    dyns_get_table_field_info(
            tag, idx, buf, sizeof(buf),
            (int *)&fld->type,
            (int *)&fld->offset, NULL,
            (int *)&fld->len, /* want fullsize, not size */
            NULL, is_ondisk_schema);

    fld->idx = -1;
    fld->name = strdup(buf);
    fld->in_default = NULL;
    fld->in_default_len = 0;
    fld->in_default_type = 0;
    fld->flags |= NO_NULL;

    /* for special on-disk schema, change lenghts to ondisk format */
    if (is_ondisk_schema) {
        *offset = _set_column_ondisk_length(fld, idx, tag, *offset);

        if ( _set_column_options(fld, idx, tag, err))
            return NULL;
    }
    return fld;
}

static int set_up_temporal_columns(struct dbtable *db, struct schema *schema)
{
    int period;
    int start;
    int end;
    int rc = 0;
    for (period = 0; period < PERIOD_MAX; period++) {
        start = -1;
        end = -1;
        rc = dyns_get_period(period, &start, &end);
        if (rc != 0) {
            return -1;
        }
        if (start >= 0 && end >= 0) {
            if ((schema->member[start].flags & MEMBER_PERIOD_MASK) ||
                (schema->member[end].flags & MEMBER_PERIOD_MASK)) {
                if (db->iq)
                    reqerrstr(db->iq, ERR_SC,
                              "period time already in use.");
                return -1;
            }
            if (schema->member[start].in_default ||
                schema->member[start].out_default ||
                schema->member[end].in_default ||
                schema->member[end].out_default) {
                if (db->iq)
                    reqerrstr(
                        db->iq, ERR_SC,
                        "period time should not have default values.");
                return -1;
            }
            if (schema->member[start].flags & NO_NULL ||
                schema->member[end].flags & NO_NULL) {
                if (db->iq)
                    reqerrstr(db->iq, ERR_SC,
                              "period time should be nullable.");
                return -1;
            }
            schema->member[start].flags |=
                (period == PERIOD_SYSTEM ? SYSTEM_START : BUSINESS_START);
            schema->member[end].flags |=
                (period == PERIOD_SYSTEM ? SYSTEM_END : BUSINESS_END);
            schema->periods[period].enable = 1;
            schema->periods[period].start = start;
            schema->periods[period].end = end;
            db->periods[period].enable = 1;
            db->periods[period].start = start;
            db->periods[period].end = end;
        }
    }

    return 0;
}

/* have a cmacc parsed and sanity-checked csc schema.
   convert to sql statement, execute, save schema in db
   structure and sanity check again (sqlite schema must
   match cmacc).  libcmacc2 puts results into globals.
   process them here after each .csc file is read
   - save cmacc structures into db structs
   */
static int add_cmacc_stmt(dbtable *tbl, int alt, int allow_ull,
        int no_side_effects, struct errstat *err)
{
    /* loaded from csc2 at this point */
    struct schema **schs;
    struct schema **schs_indx;
    int nschs = 0;
    int nschs_indx = 0;
    struct schema *sch_ondisk = NULL;
    struct schema *sch_default = NULL;
    int ntags;
    int itag;
    int field;
    int rc;
    char *tag = NULL;
    int i;
    int offset;

    ntags = dyns_get_table_count();

    /*  we will store the schemas here and add them at the end, so we 
     *  do not polute tag hash in case of error
     */
    /* a server schema per tag + one client schema for ondisk */
    schs = calloc(ntags + 1, sizeof(struct schema*));
    if (!schs) {
        errstat_set_rcstrf(err, rc = -1, "out of memory %s:%d", __func__,
                           __LINE__);
        return -1;
    }
    /* a server and a client schema per index, for ondisk and default */
    schs_indx = calloc(tbl->nix * 2 * 2, sizeof(struct schema*));
    if (!schs_indx) {
        free(schs);
        errstat_set_rcstrf(err, rc = -1, "out of memory %s:%d", __func__,
                           __LINE__);
        return -1;
    }

    /* add server schema for each tag */
    for (itag = 0; itag < ntags; itag++) {
        tag = dyns_get_table_tag(itag);

        struct schema *sch = schs[nschs++] = _create_table_schema(tag, alt);
        if (!sch) {
            errstat_set_rcstrf(err, rc = -1, "out of memory");
            goto err;
        }

        if ((gbl_morecolumns && sch->nmembers > MAXCOLUMNS) ||
            (!gbl_morecolumns && sch->nmembers > MAXDYNTAGCOLUMNS)) {
            errstat_set_rcstrf(err, rc = -1, "too many columns (max: %d)",
                               gbl_morecolumns ? MAXCOLUMNS : MAXDYNTAGCOLUMNS);
            goto err;
        }

        offset = 0;
        if (!strncasecmp(tag, ".ONDISK", 7))
            sch_ondisk = sch;
        else if (!strncasecmp(tag, ".DEFAULT", 7))
            sch_default = sch;


        /* create the columns for the schema (here, fields for tags ...) */
        for (field = 0; field < sch->nmembers; field++) {
            struct field *fld = _create_schema_column(&sch->member[field], field,
                                                      tag, sch == sch_ondisk, err, &offset);
            if (!fld)  {
                rc = -1;
                goto err;
            }

            if (_check_column_ull(fld, tbl->tablename, allow_ull)) {
                errstat_set_rcstrf(err, rc = -1, "u_longlong is not supported");
                goto err;
            }

            sch->numblobs = _set_column_blobs(fld, sch->numblobs);

            if (sch == sch_ondisk) {
                if (!no_side_effects && !strcasecmp(fld->name, "comdb2_seqno")) {
                    tbl->do_local_replication = 1;
                }
            }
        }
        /* offset is only computed for adjusted ondisk schema */
        if (sch == sch_ondisk)
            sch->recsize = offset;
        rc = set_up_temporal_columns(tbl, sch);
        if (rc)
            goto err;
    }

    /* add client schema for ondisk tag */
    if (!alt) {
        schs[nschs] = clone_server_to_client_tag(sch_ondisk, ".ONDISK_CLIENT");
    } else {
        schs[nschs] = clone_server_to_client_tag(sch_ondisk, ".NEW..ONDISK_CLIENT");
    }
    if (!schs[nschs++]) {
        rc = -1;
        goto err;
    }

    if (!no_side_effects) {
        tbl->schema = sch_ondisk;
        tbl->lrl = sch_ondisk->recsize;
        tbl->numblobs = sch_ondisk->numblobs;
        tbl->ixschema = calloc(sizeof(struct schema *), tbl->nix);
        if (!tbl->ixschema) {
            rc = -1;
            goto err;
        }
    }

    /* keys not reqd for ver temp table; just ondisk tag */
    if (strncasecmp(tbl->tablename, gbl_ver_temp_table, strlen(gbl_ver_temp_table))) {
        rc = create_keys_schemas(tbl, sch_ondisk, alt, 1, schs_indx, &nschs_indx, err);
        if (rc)
            goto err;
        if (sch_default) {
            /* comdbg clients */
            rc = create_keys_schemas(tbl, sch_default, alt, 0, schs_indx,
                                     &nschs_indx, err);
            /* NOTE: creating schemas for the indexes in the DEFAULT tag ignore the
             * case when an index has columns that are not present in the DEFAULT tag!!!
             * This is the behaviour 7.0 so I will not change it.  It is to be
             * noticed though that this leaves schema for DEFAULT tag with no indexes
             *
             * Example:
             * ondisk {
             *    int a
             *    int b
             * }
             * tag default {
             *    int a
             * }
             * keys {
             *    "a" = a -> this is something that default could use, but ..
             *    "b" = b -> this fails all indexes for default
             * }
             *
             */
            if (rc > 0)
                goto err;
        }
    }

    /* client index schemas */
    if (!no_side_effects) {
        char cname_buf[MAXTAGLEN];
        for (i = 0; i < tbl->nix; i++) {
            struct schema *idxsch = sch_ondisk->ix[i];
            if (!alt) {
                snprintf(cname_buf, sizeof(cname_buf),
                        ".ONDISK_CLIENT_IX_%d", i);
            } else {
                snprintf(cname_buf, sizeof(cname_buf),
                        ".NEW..ONDISK_CLIENT_IX_%d", i);
            }

            schs_indx[nschs_indx] =
                clone_server_to_client_tag(idxsch, cname_buf);
            if (!schs_indx[nschs_indx++]) {
                rc = -1;
                goto err;
            }
            /* keys are also in ondisk format */
            tbl->ix_keylen[i] = idxsch->member[idxsch->nmembers-1].offset +
                idxsch->member[idxsch->nmembers-1].len;
        }
    }

    /* all is ok, now make all schemas public */
    for(i=0; i < nschs; i++) {
        add_tag_schema(tbl->tablename, schs[i]);
    }
    for(i=0; i < nschs_indx; i++) {
        add_tag_schema(tbl->tablename, schs_indx[i]);
    }

    free(schs);
    free(schs_indx);
    return 0;

err:
    if (rc) {
       for(i=0; i < nschs && schs[i]; i++) {
           freeschema(schs[i], 1);
       }
       for(i=0; i < nschs_indx && schs_indx[i]; i++) {
           freeschema(schs_indx[i], 1);
       }
    }
    free(schs);
    free(schs_indx);
    return rc;
}

static int init_check_constraints(dbtable *tbl)
{
    char *consname = NULL;
    char *check_expr = NULL;
    strbuf *sql;
    int rc;

    tbl->n_check_constraints = dyns_get_check_constraint_count();
    if (tbl->n_check_constraints == 0) {
        return 0;
    }

    tbl->check_constraints =
        calloc(tbl->n_check_constraints, sizeof(check_constraint_t));
    tbl->check_constraint_query =
        calloc(tbl->n_check_constraints, sizeof(char *));
    if ((tbl->check_constraints == NULL) ||
        (tbl->check_constraint_query == NULL)) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to allocate memory\n", __func__,
               __LINE__);
        return 1;
    }

    sql = strbuf_new();

    for (int i = 0; i < tbl->n_check_constraints; i++) {
        rc = dyns_get_check_constraint_at(i, &consname, &check_expr);
        if (rc == -1)
            abort();
        tbl->check_constraints[i].consname = consname ? strdup(consname) : 0;
        tbl->check_constraints[i].expr = check_expr ? strdup(check_expr) : 0;

        strbuf_clear(sql);

        strbuf_appendf(sql, "WITH \"%s\" (\"%s\"", tbl->tablename,
                       tbl->schema->member[0].name);
        for (int j = 1; j < tbl->schema->nmembers; ++j) {
            strbuf_appendf(sql, ", %s", tbl->schema->member[j].name);
        }
        strbuf_appendf(sql, ") AS (SELECT @%s", tbl->schema->member[0].name);
        for (int j = 1; j < tbl->schema->nmembers; ++j) {
            strbuf_appendf(sql, ", @%s", tbl->schema->member[j].name);
        }
        strbuf_appendf(sql, ") SELECT (%s) FROM \"%s\"",
                       tbl->check_constraints[i].expr, tbl->tablename);
        tbl->check_constraint_query[i] = strdup(strbuf_buf(sql));
        if (tbl->check_constraint_query[i] == NULL) {
            logmsg(LOGMSG_ERROR, "%s:%d failed to allocate memory\n", __func__,
                   __LINE__);
            cleanup_newdb(tbl);
            strbuf_free(sql);
            return 1;
        }
    }
    strbuf_free(sql);
    return 0;
}
