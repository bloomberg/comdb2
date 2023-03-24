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

static dbtable *newdb_from_schema(struct dbenv *env, char *tblname, int dbnum,
                                  int dbix);
static int init_check_constraints(dbtable *tbl);
static int add_cmacc_stmt(dbtable *db, int alt, int allow_ull,
                          int no_side_effects, struct errstat *err);

struct dbtable *create_new_dbtable(struct dbenv *dbenv, char *tablename,
                                   char *csc2, int dbnum, int indx,
                                   int sc_alt_tablename, int allow_ull,
                                   int no_side_effects, struct errstat *err)
{
    struct dbtable *newtable = NULL;
    int rc;

    dyns_init_globals();

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

    newtable = newdb_from_schema(dbenv, tablename, dbnum, indx);
    if (!newtable) {
        errstat_set_rcstrf(err, -1, "newdb_from_schema failed for %s",
                           tablename);
        rc = -1;
        goto err;
    }

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

static dbtable *newdb_from_schema(struct dbenv *env, char *tblname, int dbnum,
                                  int dbix)
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

    tbl->dbs_idx = dbix;

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

char *indexes_expressions_unescape(char *expr);
extern int gbl_new_indexes;
extern char gbl_ver_temp_table[];
/* create keys for each schema */
static int create_key_schema(dbtable *db, struct schema *schema, int alt,
                             struct errstat *err)
{
    char buf[MAXCOLNAME + 1];
    int ix;
    int piece;
    struct field *m;
    int offset;
    char altname[MAXTAGLEN];
    char tmptagname[MAXTAGLEN + sizeof(".NEW.")];
    char *where;
    char *expr;
    int rc;
    int ascdesc;
    char *dbname = db->tablename;
    struct schema *s;
    struct schema *p;
    struct partial_datacopy *pd;

    /* keys not reqd for ver temp table; just ondisk tag */
    if (strncasecmp(dbname, gbl_ver_temp_table, strlen(gbl_ver_temp_table)) ==
        0)
        return 0;

    schema->nix = dyns_get_idx_count();
    schema->ix = calloc(schema->nix, sizeof(struct schema *));

    for (ix = 0; ix < schema->nix; ix++) {
        snprintf(buf, sizeof(buf), "%s_ix_%d", schema->tag, ix);
        s = schema->ix[ix] = calloc(1, sizeof(struct schema));

        s->tag = strdup(buf);
        s->nmembers = dyns_get_idx_piece_count(ix);
        s->member = calloc(schema->ix[ix]->nmembers, sizeof(struct field));

        s->flags = SCHEMA_INDEX;

        db->ix_datacopylen[ix] = 0;

        if (dyns_is_idx_dup(ix))
            s->flags |= SCHEMA_DUP;

        if (dyns_is_idx_recnum(ix))
            s->flags |= SCHEMA_RECNUM;

        if (dyns_is_idx_datacopy(ix))
            s->flags |= SCHEMA_DATACOPY;

        if (dyns_is_idx_uniqnulls(ix))
            s->flags |= SCHEMA_UNIQNULLS;

        if (dyns_is_idx_partial_datacopy(ix)) {
            s->flags |= SCHEMA_PARTIALDATACOPY;

            rc = dyns_get_idx_partial_datacopy(ix, &pd);
            if (rc == 0 && pd) {
                p = s->partial_datacopy = calloc(1, sizeof(struct schema));

                // find number of members
                int numMembers = 0;
                struct partial_datacopy *temp = pd;
                while (temp) {
                    temp = temp->next;
                    numMembers++;
                }

                p->tag = strdup("PARTIAL DATACOPY");
                p->flags = SCHEMA_PARTIALDATACOPY_ACTUAL;

                p->nmembers = numMembers;
                p->member = calloc(p->nmembers, sizeof(struct field));

                temp = pd;
                piece = 0;
                offset = 0;
                while (temp) {
                    m = &p->member[piece];
                    m->idx = find_field_idx(dbname, schema->tag, temp->field);
                    if (m->idx == -1) {
                        rc = -ix - 1;
                        goto errout;
                    }

                    m->isExpr = 0;
                    m->in_default = NULL;
                    m->out_default = NULL;
                    m->name = strdup(temp->field);
                    m->offset = offset;
                    m->flags = schema->member[m->idx].flags;
                    m->blob_index = schema->member[m->idx].blob_index;
                    m->type = schema->member[m->idx].type;
                    m->len = schema->member[m->idx].len;
                    offset += m->len;
                    memcpy(&m->convopts, &schema->member[m->idx].convopts, sizeof(struct field_conv_opts));

                    temp = temp->next;
                    piece++;
                }

                db->ix_datacopylen[ix] = offset;

            } else {
                errstat_set_rcstrf(err, -1, "cannot form partial datacopy for index %d.", ix);
                goto errout;
            }
        } else {
            s->partial_datacopy = NULL;
        }

        s->nix = 0;
        s->ix = NULL;
        s->ixnum = ix;
        if ((strcasecmp(schema->tag, ".ONDISK") == 0 ||
             strcasecmp(schema->tag, ".NEW..ONDISK") == 0) &&
            (db->ixschema && db->ixschema[ix] == NULL))
            db->ixschema[ix] = s;
        offset = 0;
        for (piece = 0; piece < s->nmembers; piece++) {
            m = &s->member[piece];
            m->flags = 0;
            expr = NULL;
            m->isExpr = 0;
            dyns_get_idx_piece(ix, piece, buf, sizeof(buf), &m->type,
                               (int *)&m->offset, (int *)&m->len, &ascdesc,
                               &expr);
            if (expr) {
                expr = indexes_expressions_unescape(expr);
                m->name = expr;
                m->isExpr = 1;
                m->idx = -1;
                if (expr == NULL) {
                    errstat_set_rcstrf(err, -1, "unterminated string literal");
                    rc = 1;
                    goto errout;
                }
                switch (m->type) {
                case 0:
                    abort();
                case SERVER_BLOB:
                case SERVER_VUTF8:
                case SERVER_BLOB2:
                    errstat_set_rcstrf(err, -1, "blob index is not supported.");
                    rc = 1;
                    goto errout;
                case SERVER_BCSTR:
                    if (m->len < 2) {
                        errstat_set_rcstrf(
                            err, -1,
                            "string must be at least 2 bytes in in length.");
                        rc = 1;
                        goto errout;
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
                    rc = 1;
                    goto errout;
                }
                db->ix_expr = 1;
            } else {
                m->name = strdup(buf);
                m->idx = find_field_idx(dbname, schema->tag, m->name);
                if (m->idx == -1) {
                    errstat_set_rcstrf(err, -1, "field %s not found in %s\n",
                                       m->name, schema->tag);
                    rc = -ix - 1;
                    goto errout;
                }
                m->type = schema->member[m->idx].type;
                m->len = schema->member[m->idx].len;

                /* the dbstore default is still needed during schema change, so
                 * populate that */
                if (schema->member[m->idx].in_default) {
                    m->in_default =
                        malloc(schema->member[m->idx].in_default_len);
                    m->in_default_len = schema->member[m->idx].in_default_len;
                    m->in_default_type = schema->member[m->idx].in_default_type;
                    memcpy(m->in_default, schema->member[m->idx].in_default,
                           m->in_default_len);
                }
                memcpy(&m->convopts, &schema->member[m->idx].convopts,
                       sizeof(struct field_conv_opts));
                if (gbl_new_indexes && strncasecmp(dbname, "sqlite_stat", 11) &&
                    strcasecmp(dbname, "comdb2_oplog") &&
                    strcasecmp(dbname, "comdb2_commit_log") &&
                    (!gbl_replicate_local ||
                     strcasecmp(m->name, "comdb2_seqno"))) {
                    m->isExpr = 1;
                    db->ix_partial = 1;
                    db->ix_expr = 1;
                }
            }
            if (ascdesc)
                m->flags |= INDEX_DESCEND;
            /* we could check here if there are decimals in the index, and mark
             * it "datacopy" special */
            if (db->ix_datacopy[ix] == 0 && m->type == SERVER_DECIMAL) {
                db->ix_collattr[ix]++; /* special tail */
            } else if (m->type == SERVER_DECIMAL) {
                db->ix_datacopy[ix]++; /* special tail */
            }
            m->offset = offset;
            offset += m->len;
        }
        /* rest of fields irrelevant for indexes */
        add_tag_schema(dbname, s);
        rc = dyns_get_idx_tag(ix, altname, MAXTAGLEN, &where);
        if (rc == 0 && (strcasecmp(schema->tag, ".ONDISK") == 0 ||
                        strcasecmp(schema->tag, ".NEW..ONDISK") == 0)) {
            if (alt == 0) {
                add_tag_alias(dbname, s, altname, schema->nmembers);
            } else {
                snprintf(tmptagname, sizeof(tmptagname), ".NEW.%s", altname);
                add_tag_alias(dbname, s, tmptagname, schema->nmembers);
            }
            if (where) {
                s->where = strdup(where);
                db->ix_partial = 1;
            }
        }
    }
    return 0;

errout:
    freeschema(s);
    free(schema->ix);
    schema->ix = NULL;
    schema->nix = 0;
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

/* have a cmacc parsed and sanity-checked csc schema.
   convert to sql statement, execute, save schema in db
   structure and sanity check again (sqlite schema must
   match cmacc).  libcmacc2 puts results into globals.
   process them here after each .csc file is read
 */
static int add_cmacc_stmt(dbtable *db, int alt, int allow_ull,
                          int no_side_effects, struct errstat *err)
{
    /* loaded from csc2 at this point */
    int field;
    int rc;
    struct schema *schema;
    char buf[MAXCOLNAME + 1] = {0}; /* scratch space buffer */
    int offset;
    int ntags;
    int itag;
    char *tag = NULL;
    int isnull;
    char tmptagname[MAXTAGLEN] = {0};
    char *rtag = NULL;
    int have_comdb2_seqno_field;

    /* save cmacc structures into db structs */

    /* table */
    ntags = dyns_get_table_count();
    for (itag = 0; itag < ntags; itag++) {
        have_comdb2_seqno_field = 0;
        if (rtag) {
            free(rtag);
            rtag = NULL;
        }

        if (alt == 0) {
            tag = strdup(dyns_get_table_tag(itag));
            rtag = strdup(tag);
        } else {
            rtag = strdup(dyns_get_table_tag(itag));
            snprintf(tmptagname, sizeof(tmptagname), ".NEW.%s", rtag);
            tag = strdup(tmptagname);
        }
        schema = calloc(1, sizeof(struct schema));
        schema->tag = tag;

        add_tag_schema(db->tablename, schema);

        schema->nmembers = dyns_get_table_field_count(rtag);
        if ((gbl_morecolumns && schema->nmembers > MAXCOLUMNS) ||
            (!gbl_morecolumns && schema->nmembers > MAXDYNTAGCOLUMNS)) {
            errstat_set_rcstrf(err, -1, "too many columns (max: %d)",
                               gbl_morecolumns ? MAXCOLUMNS : MAXDYNTAGCOLUMNS);
            return -1;
        }
        schema->member = calloc(schema->nmembers, sizeof(struct field));
        schema->flags = SCHEMA_TABLE;
        schema->recsize = dyns_get_table_tag_size(rtag); /* wrong for .ONDISK */
        /* we generate these later */
        schema->ix = NULL;
        schema->nix = 0;
        offset = 0;
        int is_disk_schema = (strncasecmp(rtag, ".ONDISK", 7) == 0);

        for (field = 0; field < schema->nmembers; field++) {
            int padval;
            int type;
            int sz;

            dyns_get_table_field_info(
                rtag, field, buf, sizeof(buf),
                (int *)&schema->member[field].type,
                (int *)&schema->member[field].offset, NULL,
                (int *)&schema->member[field].len, /* want fullsize, not size */
                NULL, is_disk_schema);
            schema->member[field].idx = -1;
            schema->member[field].name = strdup(buf);
            schema->member[field].in_default = NULL;
            schema->member[field].in_default_len = 0;
            schema->member[field].in_default_type = 0;
            schema->member[field].flags |= NO_NULL;

            extern int gbl_forbid_ulonglong;
            if (gbl_forbid_ulonglong &&
                (schema->member[field].type == SERVER_UINT ||
                 schema->member[field].type == CLIENT_UINT) &&
                schema->member[field].len == sizeof(unsigned long long)) {
                logmsg(LOGMSG_ERROR,
                       "Error in table %s: u_longlong is unsupported\n",
                       db->tablename);
                /* Skip returning error on presence of u_longlong if:

                   - we haven't fully started, or
                   - we were explicitly asked (by the callers) to do so

                   This is done to allow existing databases with tables
                   containing u_longlong to start and also certain operational
                   commands, like table rebuilds, truncates and time partition
                   rollouts, to succeed.
                */
                if (gbl_ready && !allow_ull) {
                    errstat_set_rcstrf(err, -1, "u_longlong is not supported");
                    return -1;
                }
            }

            /* count the blobs */
            if (schema->member[field].type == CLIENT_BLOB ||
                schema->member[field].type == CLIENT_VUTF8 ||
                schema->member[field].type == CLIENT_BLOB2 ||
                schema->member[field].type == SERVER_BLOB ||
                schema->member[field].type == SERVER_BLOB2 ||
                schema->member[field].type == SERVER_VUTF8) {
                schema->member[field].blob_index = schema->numblobs;
                schema->numblobs++;
            } else {
                schema->member[field].blob_index = -1;
            }

            /* for special on-disk schema, change types to be
               special on-disk types */
            if (is_disk_schema) {
                if (strcasecmp(schema->member[field].name, "comdb2_seqno") == 0)
                    have_comdb2_seqno_field = 1;

                /* cheat: change type to be ondisk type */
                switch (schema->member[field].type) {
                case SERVER_BLOB:
                    /* TODO use the enums that are used in types.c */
                    /* blobs are stored as flag byte + 32 bit length */
                    schema->member[field].len = 5;
                    break;
                case SERVER_VUTF8:
                case SERVER_BLOB2:
                    /* TODO use the enums that are used in types.c */
                    /* vutf8s are stored as flag byte + 32 bit length, plus
                     * optionally strings up to a certain length can be stored
                     * in the record itself */
                    if (schema->member[field].len == -1)
                        schema->member[field].len = 5;
                    else
                        schema->member[field].len += 5;
                    break;
                case SERVER_BCSTR: {
                    int clnt_type = 0;
                    int clnt_offset = 0;
                    int clnt_len = 0;

                    dyns_get_table_field_info(rtag, field, buf, sizeof(buf),
                                              &clnt_type, &clnt_offset, NULL,
                                              &clnt_len, NULL, 0);

                    if (clnt_type == CLIENT_PSTR || clnt_type == CLIENT_PSTR2) {
                        schema->member[field].len++;
                    } else {
                        /* no change needed from client length */
                    }
                } break;
                case SERVER_DATETIME:
                    /* server CLIENT_DATETIME is a server_datetime_t */
                    schema->member[field].len = sizeof(server_datetime_t);
                    break;
                case SERVER_DATETIMEUS:
                    /* server CLIENT_DATETIMEUS is a server_datetimeus_t */
                    schema->member[field].len = sizeof(server_datetimeus_t);
                    break;
                case SERVER_INTVYM:
                    /* server CLIENT_INTVYM is a server_intv_ym_t */
                    schema->member[field].len = sizeof(server_intv_ym_t);
                    break;
                case SERVER_INTVDS:
                    /* server CLIENT_INTVDS is a server_intv_ds_t */
                    schema->member[field].len = sizeof(server_intv_ds_t);
                    break;
                case SERVER_INTVDSUS:
                    /* server CLIENT_INTVDSUS is a server_intv_dsus_t */
                    schema->member[field].len = sizeof(server_intv_dsus_t);
                    break;
                case SERVER_DECIMAL:
                    switch (schema->member[field].len) {
                    case 14:
                        schema->member[field].len = sizeof(server_decimal32_t);
                        break;
                    case 24:
                        schema->member[field].len = sizeof(server_decimal64_t);
                        break;
                    case 43:
                        schema->member[field].len = sizeof(server_decimal128_t);
                        break;
                    default:
                        abort();
                    }
                    break;
                default:
                    /* other types just add one for the flag byte */
                    schema->member[field].len++;
                    break;
                }
                schema->member[field].offset = offset;
                offset += schema->member[field].len;

                /* correct recsize */
                if (field == schema->nmembers - 1) {
                    /* we are on the last field */
                    schema->recsize = offset;
                }
#if 0
              schema->member[field].type = 
                  client_type_to_server_type(schema->member[field].type);
#endif
                if (!no_side_effects) {
                    db->schema = schema;
                    if (db->ixschema)
                        free(db->ixschema);
                    db->ixschema = calloc(sizeof(struct schema *), db->nix);

                    db->do_local_replication = have_comdb2_seqno_field;
                }
            }

            if (strcmp(rtag, ".ONDISK") == 0) {
                /* field allowed to be null? */
                rc = dyns_get_table_field_option(rtag, field, FLDOPT_NULL,
                                                 &type, &sz, &isnull,
                                                 sizeof(int), NULL);
                if (rc == 0 && isnull)
                    schema->member[field].flags &= ~NO_NULL;

                /* dbpad value (used for byte array conversions).  If it is
                 * present
                 * then type must be integer. */
                rc = dyns_get_table_field_option(rtag, field, FLDOPT_PADDING,
                                                 &type, &sz, &padval,
                                                 sizeof(padval), NULL);
                if (rc == 0 && CLIENT_INT == type) {
                    schema->member[field].convopts.flags |= FLD_CONV_DBPAD;
                    schema->member[field].convopts.dbpad = padval;
                }

                /* input default */
                rc = init_default_value(&schema->member[field], field,
                                        FLDOPT_DBSTORE);
                if (rc != 0) {
                    if (rtag)
                        free(rtag);
                    errstat_set_rcstrf(err, -1, "invalid default column value");
                    return -1;
                }

                /* output default  */
                rc = init_default_value(&schema->member[field], field,
                                        FLDOPT_DBLOAD);
                if (rc != 0) {
                    if (rtag)
                        free(rtag);
                    errstat_set_rcstrf(err, -1, "invalid dbpad value");
                    return -1;
                }
            }
        }
        if (create_key_schema(db, schema, alt, err) > 0)
            return -1;
        if (is_disk_schema) {
            int i, rc;
            /* csc2 doesn't have the correct recsize for ondisk schema - use
             * our value instead. */
            schema->recsize = offset;
            if (!alt) {
                if (!no_side_effects)
                    db->lrl =
                        get_size_of_schema_by_name(db->tablename, ".ONDISK");
                rc = clone_server_to_client_tag(db->tablename, ".ONDISK",
                                                ".ONDISK_CLIENT");
            } else {
                if (!no_side_effects)
                    db->lrl = get_size_of_schema_by_name(db->tablename,
                                                         ".NEW..ONDISK");
                rc = clone_server_to_client_tag(db->tablename, ".NEW..ONDISK",
                                                ".NEW..ONDISK_CLIENT");
            }
            if (rc)
                logmsg(LOGMSG_ERROR,
                       "clone_server_to_client_tag returns error rc=%d", rc);

            if (!no_side_effects) {
                char sname_buf[MAXTAGLEN], cname_buf[MAXTAGLEN];
                db->nix = dyns_get_idx_count();
                for (i = 0; i < db->nix; i++) {
                    if (!alt)
                        snprintf(tmptagname, sizeof(tmptagname),
                                 ".ONDISK_ix_%d", i);
                    else
                        snprintf(tmptagname, sizeof(tmptagname),
                                 ".NEW..ONDISK_ix_%d", i);
                    db->ix_keylen[i] =
                        get_size_of_schema_by_name(db->tablename, tmptagname);

                    if (!alt) {
                        snprintf(sname_buf, sizeof(sname_buf), ".ONDISK_IX_%d",
                                 i);
                        snprintf(cname_buf, sizeof(cname_buf),
                                 ".ONDISK_CLIENT_IX_%d", i);
                    } else {
                        snprintf(sname_buf, sizeof(sname_buf),
                                 ".NEW..ONDISK_IX_%d", i);
                        snprintf(cname_buf, sizeof(cname_buf),
                                 ".NEW..ONDISK_CLIENT_IX_%d", i);
                    }

                    clone_server_to_client_tag(db->tablename, sname_buf,
                                               cname_buf);
                }

                db->numblobs = schema->numblobs;
            }
        }
    }
    if (rtag)
        free(rtag);
    return 0;
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
