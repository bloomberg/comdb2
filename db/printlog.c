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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <arpa/inet.h>

#include "db.h"
#include "db_int.h"
#include "db_page.h"

#include "dbinc/fop.h"
#include "dbinc/hash.h"
#include "dbinc/log.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

#include "llog_auto.h"
#include "llog_int.h"
#include "llog_auto.h"

#include "btree.h"
#include "db_swap.h"
#include "dbinc_auto/btree_auto.h"

#include "bdb_int.h"
#include "plhash.h"
#include "endian_core.h"
#include "comdb2.h"

#include <bdb_api.h>
#include <bdb_int.h>

struct fname {
    char *fname;
    char *table;
    int index;
    int dta;
    int stripe;
    struct dbtable *db;
};

struct fnames {
    int nnames;
    struct fname **names;
};

void dump_record(DB_ENV *dbenv, __db_addrem_args *args, struct fname *f)
{
    struct schema *sc;
    char tag[100];
    /* just do data */
    if (f->index == -1 && args->opcode == DB_ADD_DUP) {
        unsigned long long genid = 0;
        if (args->indx % 2 == 0) {
            buf_get(&genid, 8, args->dbt.data,
                    (int8_t *)args->dbt.data + args->dbt.size);
            printf("genid %016llx ", genid);
        } else if (f->db) {
            /* decode data record */
            bdb_state_type *bdb_state;
            bdb_state = (bdb_state_type *)f->db->handle;
            struct odh odh;
            int rc;
            void *freep = NULL;

            rc = bdb_unpack(bdb_state, args->dbt.data, args->dbt.size, NULL, 0,
                            &odh, &freep);
            if (rc) {
                printf("can't unpack record ");
                return;
            }

            char tag[gbl_ondisk_ver_len];

            if (odh.csc2vers == 0) {
                sprintf(tag, ".ONDISK");
            } else {
                sprintf(tag, gbl_ondisk_ver_fmt, odh.csc2vers);
            }
            sc = find_tag_schema(f->table, tag);
            if (sc == NULL) {
                printf("can't find schema for %s ", tag);
                return;
            }
            printrecord(odh.recptr, sc, odh.length);

            if (freep)
                free(freep);
        }
    }
}

int print_addrem(DB_ENV *dbenv, DBT *logdta, DB_LSN *lsn, db_recops op, void *p)
{
    __db_addrem_args *args;
    DB_LOG *logp;
    int ret;
    struct fnames *fnp = (struct fnames *)p;
    struct fname *f;

    logp = dbenv->lg_handle;

    ret = __db_addrem_read(dbenv, logdta->data, &args);
    if (ret) {
        printf("%u:%u addrem ???\n", lsn->file, lsn->offset);
        return 0;
    }
    if (IS_REM_OPCODE(args->opcode)) {
        printf("del hdr indx %u:\n", args->indx);
        fsnapf(stdout, args->hdr.data, args->hdr.size);
    }

    if (args->fileid >= fnp->nnames || fnp->names[args->fileid] == NULL) {
        printf("%u:%u %x addrem fileid %d name ???n", lsn->file, lsn->offset,
               args->txnid->txnid, args->fileid);
        return 0;
    }
    f = fnp->names[args->fileid];

    char *opcode;
    if (args->opcode == DB_ADD_DUP)
        opcode = "add";
    else if (IS_REM_OPCODE(args->opcode)) {
        opcode = "del";
    } else
        opcode = "addrem???";

    printf("%u:%u %s %x prev %u:%u table %s ", lsn->file, lsn->offset, opcode,
           args->txnid->txnid, args->prev_lsn.file, args->prev_lsn.offset,
           f->table ? f->table : f->fname);
    if (f->index != -1)
        printf("index %d ", f->index);

    if (f->table) {
        dump_record(dbenv, args, f);
    }

    printf("\n");

    free(args);
    return 0;
}

int print_commit(DB_ENV *dbenv, DBT *logdta, DB_LSN *lsn, db_recops op,
                 void *unused)
{
    __txn_regop_args *args;
    int ret;

    ret = __txn_regop_read(dbenv, logdta->data, &args);
    if (ret) {
        printf("%u:%u commit ??? ret %d\n", lsn->file, lsn->offset, ret);
        return 0;
    }

    printf("%u:%u commit %x\n", lsn->file, lsn->offset, args->txnid->txnid);
    free(args);
    return 0;
}

int print_ignore(DB_ENV *dbenv, DBT *logdta, DB_LSN *lsn, db_recops op,
                 void *unused)
{
    uint32_t type = 0;
    if (logdta->size > sizeof(uint32_t))
        LOGCOPY_32(&type, logdta->data);
    // printf("%u:%u type %u\n", lsn->file, lsn->offset, type);
    return 0;
}

/* From a filename, determined table/index/stripe/blob.  Indicate failure by not
 * setting newname->table */
void set_table_info_from_filename(DB_ENV *dbenv, struct fname *newname,
                                  char *fname, int len)
{
    char *start, *end;
    char *type;
    bdb_state_type *bdb_state;
    bdb_state_type *table = NULL;
    int i;

    bzero(newname, sizeof(struct fname));

    newname->fname = calloc(1, len + 1);
    memcpy(newname->fname, fname, len);

    /* not a mangled name? -- ignore */
    if (strstr(fname, "XXX.") != fname)
        return;

    /* XXX.sqlite_stat4_53c55824000c0000.index
     *
     *    ^start       ^end              ^type
     * */

    bdb_state = (bdb_state_type *)dbenv->app_private;
    if (bdb_state == NULL)
        return;

    start = newname->fname + 4;
    end = strrchr(newname->fname, '_');
    if (end == NULL)
        return;
    type = strchr(end, '.');
    if (type == NULL)
        return;
    type++;

    for (int i = 0; i < bdb_state->numchildren; i++) {
        if (strncmp(bdb_state->children[i]->name, start, end - start) == 0)
            table = bdb_state->children[i];
    }

    if (table == NULL)
        return;

    if (strncmp(type, "datas", 5) == 0) {
        newname->index = -1;
        newname->dta = 0;
        newname->stripe = atoi(type + 5); /* error if no stripe? */
    } else if (strncmp(type, "blobs", 5) == 0 ||
               strncmp(type, "blob", 4) == 0) {
        int stripe;

        if (strncmp(type, "blob", sizeof("blob")) == 0)
            stripe = 0;
        else {
            stripe = atoi(type + 5);
            if (stripe > MAXSTRIPE)
                return;
        }

        /* numdtafiles = numblobs */
        for (i = 0; i < table->numdtafiles; i++) {
            /* we don't have a way to map back from filename to blob number,
             * so go through all blobs in the table and compare */
            if (strncmp(fname, table->dbp_data[i][stripe]->fname, len) == 0) {
                newname->index = -1;
                newname->dta = i;
                newname->stripe = 0;
                break;
            }
        }
        if (i == table->numdtafiles)
            return;
    } else if (strncmp(type, "index", 5) == 0) {
        for (i = 0; i < table->numix; i++) {
            if (strncmp(fname, table->dbp_ix[i]->fname, len) == 0) {
                newname->index = i;
                newname->dta = -1;
                newname->stripe = -1;
                break;
            }
        }
        if (i == table->numix)
            return;
    }

    newname->table = calloc(1, end - start + 1);
    strncpy(newname->table, start, end - start);

    newname->db = get_dbtable_by_name(newname->table);
}

int print_register(DB_ENV *dbenv, DBT *logdta, DB_LSN *lsn, db_recops op,
                   void *p)
{
    __dbreg_register_args *args;
    int ret;
    struct fnames *fnp = (struct fnames *)p;
    struct fname *newname;

    ret = __dbreg_register_read(dbenv, logdta->data, &args);
    if (ret) {
        printf("%u:%u register ???\n", lsn->file, lsn->offset);
        return 0;
    }
    if (args->fileid >= fnp->nnames) {
        struct fname **names =
            realloc(fnp->names, (args->fileid + 1) * sizeof(struct fname *));
        for (int i = fnp->nnames; i < args->fileid + 1; i++)
            names[i] = NULL;
        fnp->names = names;
        fnp->nnames = args->fileid + 1;
    }
    newname = NULL;
    if (fnp->names[args->fileid] && strncmp(fnp->names[args->fileid]->fname,
                                            args->name.data, args->name.size)) {
        free(fnp->names[args->fileid]->fname);
        free(fnp->names[args->fileid]->table);
        newname = fnp->names[args->fileid];
        newname->fname = NULL;
        newname->table = NULL;
    } else if (fnp->names[args->fileid] == NULL) {
        newname = fnp->names[args->fileid] = calloc(1, sizeof(struct fname));
    }
    if (newname) {
        set_table_info_from_filename(dbenv, newname, args->name.data,
                                     args->name.size);
    }

    printf("%u:%u register %x %d->%.*s%s\n", lsn->file, lsn->offset,
           args->txnid->txnid, args->fileid, args->name.size, args->name.data,
           newname ? " (new/changed)" : "");

    free(args);

    return 0;
}

int printlog(bdb_state_type *bdb_state, int startfile, int startoff,
             int endfile, int endoff)
{
    DB_ENV *dbenv = bdb_state->dbenv;
    DB_LOGC *logc = NULL;
    int ret;
    DB_LSN start = {.file = startfile, .offset = startoff},
           end = {.file = endfile, .offset = endoff};
    DBT logdta = {0};
    DB_LSN lsn;
    int (**dtab)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *) = NULL;
    size_t dtabsize = 0;
    struct fnames fnames = {.nnames = 0, .names = NULL};

    logdta.flags = DB_DBT_REALLOC;

    ret = dbenv->log_cursor(dbenv, &logc, 0);
    if (ret)
        goto err;

    int rectypes[] = {
        DB___txn_ckp, DB___txn_child, DB___txn_xa_regop, DB___txn_recycle,
        DB___db_big, DB___db_ovref, DB___db_relink, DB___db_debug, DB___db_noop,
        DB___db_pg_alloc, DB___db_pg_free, DB___db_cksum, DB___db_pg_freedata,
        DB___db_pg_prepare, DB___db_pg_new, DB___crdel_metasub, DB___fop_create,
        DB___fop_remove, DB___fop_write, DB___fop_rename, DB___fop_file_remove,
        DB___ham_insdel, DB___ham_newpage, DB___ham_splitdata, DB___ham_replace,
        DB___ham_copypage, DB___ham_metagroup, DB___ham_groupalloc,
        DB___ham_curadj, DB___ham_chgpg, DB___qam_incfirst, DB___qam_mvptr,
        DB___qam_del, DB___qam_add, DB___qam_delext, DB___bam_split,
        DB___bam_rsplit, DB___bam_adj, DB___bam_cadjust, DB___bam_cdel,
        DB___bam_repl, DB___bam_root, DB___bam_curadj, DB___bam_rcuradj,
        DB___bam_prefix, DB___db_addrem, DB___db_big, DB___db_ovref,
        DB___db_relink, DB___db_debug, DB___db_noop, DB___db_pg_alloc,
        DB___db_pg_free, DB___db_cksum, DB___db_pg_freedata, DB___db_pg_prepare,
        DB___db_pg_new, DB___crdel_metasub, DB___dbreg_register,
        DB___fop_create, DB___fop_remove, DB___fop_write, DB___fop_rename,
        DB___fop_file_remove, DB___ham_insdel, DB___ham_newpage,
        DB___ham_splitdata, DB___ham_replace, DB___ham_copypage,
        DB___ham_metagroup, DB___ham_groupalloc, DB___ham_curadj,
        DB___ham_chgpg, DB___qam_incfirst, DB___qam_mvptr, DB___qam_del,
        DB___qam_add, DB___qam_delext, DB___bam_split, DB___bam_rsplit,
        DB___bam_adj, DB___bam_cadjust, DB___bam_cdel, DB___bam_repl,
        DB___bam_root, DB___bam_curadj, DB___bam_rcuradj, DB___bam_prefix,

        DB_llog_savegenid, DB_llog_scdone, DB_llog_undo_add_dta,
        DB_llog_undo_add_ix, DB_llog_ltran_commit, DB_llog_ltran_start,
        DB_llog_ltran_comprec, DB_llog_undo_del_dta, DB_llog_undo_del_ix,
        DB_llog_undo_upd_dta, DB_llog_undo_upd_ix, DB_llog_repblob,
        DB_llog_undo_add_dta_lk, DB_llog_undo_add_ix_lk,
        DB_llog_undo_del_dta_lk, DB_llog_undo_del_ix_lk,
        DB_llog_undo_upd_dta_lk, DB_llog_undo_upd_ix_lk,
    };

    for (int i = 0; i < sizeof(rectypes) / sizeof(rectypes[0]); i++)
        __db_add_recovery(dbenv, &dtab, &dtabsize, print_ignore, rectypes[i]);

    ret = __db_add_recovery(dbenv, &dtab, &dtabsize, print_addrem,
                            DB___db_addrem);
    if (ret) {
        fprintf(stderr, "__db_add_recovery DB___db_addrem ret %d\n",
                DB___db_addrem);
        goto err;
    }
    ret = __db_add_recovery(dbenv, &dtab, &dtabsize, print_commit,
                            DB___txn_regop);
    if (ret) {
        fprintf(stderr, "__db_add_recovery DB___db_addrem ret %d\n",
                DB___txn_regop);
        goto err;
    }
    ret = __db_add_recovery(dbenv, &dtab, &dtabsize, print_register,
                            DB___dbreg_register);
    if (ret) {
        fprintf(stderr, "__db_add_recovery DB___dbreg_register ret %d\n",
                DB___txn_regop);
        goto err;
    }

    /* start search */
    if (IS_ZERO_LSN(start)) {
        ret = logc->get(logc, &start, &logdta, DB_FIRST);
    } else {
        ret = logc->get(logc, &lsn, &logdta, DB_SET);
        if (ret == DB_NOTFOUND) {
            ret = logc->get(logc, &lsn, &logdta, DB_FIRST);
        }
    }
    if (ret) {
        fprintf(stderr, "logc->get ret %d\n", ret);
        goto err;
    }
    do {
        __db_dispatch(dbenv, dtab, dtabsize, &logdta, &lsn, DB_TXN_PRINT,
                      &fnames);
        ret = logc->get(logc, &lsn, &logdta, DB_NEXT);
    } while (ret == 0 && log_compare(&lsn, &end));
    if (ret != DB_NOTFOUND) {
        fprintf(stderr, "logc next ret %d\n", ret);
    }

err:
    if (logc)
        logc->close(logc, 0);
    if (logdta.data)
        free(logdta.data);
    return ret;
}
