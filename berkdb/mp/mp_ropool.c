/*
   Copyright 2022 Bloomberg Finance L.P.

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

#include "db_config.h"
#include "db_int.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"
#include "dbinc/lock.h"

#include "locks_wrap.h"
#include "dbinc_auto/lock_ext.h"
#include "dbinc/db_swap.h"

#include "tohex.h"
#include "printformats.h"

int DEBUG_PAGES = 1;

static int create_pagelist_in_mpro(DBC *dbc, db_pgno_t pgno, MPRO_PAGE_LIST **pages);
static int find_page_for_cursor(MPRO_PAGE_LIST *pagelist, DBC *dbc, db_pgno_t pgno, PAGE **page);
static void destroy_pagelist(DB_ENV *dbenv, MPRO_PAGE_LIST *pagelist);
static void mpro_panic(DB_ENV *dbenv);
static void verify_pagelist_order(DB_ENV *dbenv, MPRO_PAGE_LIST *pagelist);
static char* name_page(DB_ENV *dbenv, MPRO_PAGE_LIST *pagelist, char *out);
static int get_page_at_txnid(DB_ENV *dbenv, PAGE *pg, DB_LSN *want_lsn);

/*
 *  PUBLIC: int __mempro_fget
 *  PUBLIC:     __P((DBC *, db_pgno_t, void *));
 */
int __mempro_fget(DBC *dbc, db_pgno_t pgno, void *page) {
    MPRO_KEY k;
    DB *dbp = dbc->dbp;
    DB_ENV *dbenv = dbp->dbenv;
    DB_MPRO *mpro = dbenv->mpro;
    DB_MPOOLFILE *mpf = dbp->mpf;
    int ret = 0;
    PAGE *h;
    int nopages = 0;
    MPRO_PAGE_LIST *pages = NULL;

    if (0) {
fail:
        printf("fail\n");
    }

    k.pgno = pgno;
    memcpy(k.ufid, mpf->fileid, DB_FILE_ID_LEN);
    Pthread_mutex_lock(&mpro->mpro_mutexp);
    pages = hash_find(mpro->pages, &k);
    if (pages == NULL) {
        ret = create_pagelist_in_mpro(dbc, pgno, &pages);
        nopages = 1;
        if (ret) {
            goto err;
        }
    }
    ret = find_page_for_cursor(pages, dbc, pgno, &h);
    if (ret) {
        goto err;
    }
    verify_pagelist_order(dbenv, pages);
    *(void**)page = h;
    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    return 0;

err:
    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    // There's no separate lru for page lists - we remove the page list if there's no more pages.
    // If we created a page list but failed to add a page to it, remove it here.
    if (nopages && pages && pages->pages.count == 0) {
        hash_del(mpro->pages, pages);
        free(pages);
    }
    goto fail;
    return ret;
}

// Assert that pages in a pagelist are in commit order
// TODO: cache lsn in page header
static void verify_pagelist_order(DB_ENV *dbenv, MPRO_PAGE_LIST *pagelist) {
    MPRO_PAGE_HEADER *hdr;
    DB_LSN prev, current;
    int ret;
    char pgname[100];

    ZERO_LSN(prev);

    for (hdr = pagelist->pages.top; hdr; hdr = hdr->commit_order.next) {
        ret = __mempro_get_commit_lsn_for_txn(dbenv, TXNID((PAGE*) hdr->page), &current);
        // if we get to a transaction old enough that we don't have a record of it anymore, we're done
        if (ret)
            break;
        if (log_compare(&prev, &current) >= 0) {
            printf("%s failed for pagelist: %s  %u:%u >= %u:%u\n", __func__,
                   name_page(dbenv, pagelist, pgname), prev.file, prev.offset, current.file, current.offset);
            abort();
        }
        prev = current;
    }
}

// Find a page image at the right LSN.
static int find_page_for_cursor(MPRO_PAGE_LIST *pagelist, DBC *dbc, db_pgno_t pgno, PAGE **page) {
    DB *dbp = dbc->dbp;
    DB_ENV *dbenv = dbp->dbenv;
    DB_MPOOLFILE *mpf = dbp->mpf;
    MPRO_PAGE_HEADER *hdr;
    DB_MPRO *mpro = dbenv->mpro;
    DB_LOCK_ILOCK lk;
    DB_LOCK lock;
    PAGE *h;

    // This is the txnid we're targetting
    DB_LSN snapshot_lsn = dbc->snapshot_lsn;

    int have_lock = 0, have_page = 0, have_page_image = 0;
    int ret;

    if (DEBUG_PAGES)
        printf("%s pgno %u lsn "PR_LSN"\n", __func__, pgno, PARM_LSN(dbc->snapshot_lsn));

    // Walk a list of any cached versions of this page we have.
    hdr = pagelist->pages.top;
    while (hdr) {
        h = (PAGE*) hdr->page;
        DB_LSN current_lsn;
        // TODO: cache last?
        ret = __mempro_get_commit_lsn_for_txn(dbenv, h->txnid, &current_lsn);
        if (log_compare(&current_lsn, &snapshot_lsn) <= 0) {
            // We found a page that's older than our transaction. Now we need to check that there's no newer
            // transaction that also modified the page.  That we can use. If there isn't, this is the right version.
            // The check is easy if we have a newer transaction in the list preceding us - just check that it's previous
            // id is us.  If it is, we're done.  If not, there may be transactions that modified this page between us
            // and the version in the list.  The previous page is newer than what we need (or we would have stopped
            // there). So start there and roll back to the version we need. If there's no previous version, we need to
            // start somewhere so grab it from the "real" buffer pool.
            if (hdr->commit_order.prev && ((PAGE*) hdr->commit_order.prev->page)->prev_txnid == h->txnid) {
                if (hdr->pin == 0) {
                    listc_rfl(&mpro->pagelru, hdr);
                }
                hdr->pin++;
                *page = (PAGE*) hdr->page;
                if (DEBUG_PAGES)
                    printf("  found ok\n");
                return 0;
            }
            break;
        }
        hdr = hdr->commit_order.next;
    }

    // We need to get a base image for the page.
    do {
        hdr = mspace_malloc(dbenv->mpro->msp, offsetof(MPRO_PAGE_HEADER, page) + dbp->pgsize);
        if (hdr == NULL) {
            MPRO_PAGE_LIST *lrulist;
            MPRO_PAGE_HEADER *lruhdr;
            lruhdr = listc_rtl(&mpro->pagelru);
            if (lruhdr == NULL) {
                mpro_panic(dbenv);
                ret = ENOMEM;
                if (DEBUG_PAGES)
                    printf("  can't allocate memory for page image\n");
                goto err;
            }
            lrulist = lruhdr->pagelist;
            listc_rfl(&lrulist->pages, lruhdr);
            if (lrulist->pages.count == 0) {
                hash_del(mpro->pages, lruhdr);
                destroy_pagelist(dbenv, lrulist);
            }
            mspace_free(mpro->msp, lruhdr);
        }
    } while (hdr == NULL);
    have_page_image = 1;
    hdr->pin = 1;
    hdr->pagelist = pagelist;
    hdr->commit_order.next = hdr->commit_order.prev = NULL;
    have_page_image = 1;

    if (hdr->commit_order.prev) {
        // we have a newer version of the page - recover from that
        memcpy(hdr->page, hdr->commit_order.prev->page, dbp->pgsize);
    }
    else {
        // Get from buffer pool, if we can.
        lk.pgno = pgno;
        memcpy(lk.fileid, mpf->fileid, DB_FILE_ID_LEN);
        lk.type = DB_PAGE_LOCK;

        DBT lock_dbt = {0};
        lock_dbt.data = &lk;
        lock_dbt.size = sizeof(lk);
        ret = __lock_get(dbenv, dbc->locker, DB_LOCK_NOWAIT, &lock_dbt, DB_LOCK_READ, &lock);
        if (ret == DB_LOCK_NOTGRANTED) {
            // something has this lock - we can't get the page from the buffer pool safely
            // HERE: get page anyway, checksum until stable? Get from disk, as long as newer than what we need?
            //       Add read/write lock intents to buffer pool?
            if (DEBUG_PAGES)
                printf("  can't get lock on page, page locked\n");
            goto err;
        }
        else if (ret) {
            if (DEBUG_PAGES)
                printf("  can't get lock on page, error %d\n", ret);
            goto err;
        }
        have_lock = 1;

        ret = __memp_fget(mpf, &pgno, 0, &h);
        if (ret) {
            if (DEBUG_PAGES)
                printf("  can't get page in buffer pool\n");
            goto err;
        }
        have_page = 1;
        ret = __lock_put(dbenv, &lock);
        if (ret) {
            if (DEBUG_PAGES)
                printf("  can't release lock on page\n");
            goto err;
        }
        have_lock = 0;
        memcpy(hdr->page, h, dbp->pgsize);
        ret = __memp_fput(mpf, h, 0);
        have_page = 0;
        if (ret) {
            if (DEBUG_PAGES)
                printf("  can't return page to buffer pool\n");
            goto err;
        }
    }

    // We now have a newer but still wrong page image.  Recover to the version we want
    ret = get_page_at_txnid(dbenv, (PAGE*) hdr->page, &dbc->snapshot_lsn);
    if (ret) {
        if (DEBUG_PAGES)
            printf("  can't recover page at right LSN from image\n");
        goto err;
    }

    *page = (PAGE*) hdr->page;
    listc_atl(&pagelist->pages, hdr);
    return 0;

err:
    if (have_page)
        __memp_fput(mpf, h, 0);
    if (have_lock)
        __lock_put(dbenv, &lock);
    if (have_page_image)
        mspace_free(mpro->msp, hdr);
    return ret;
}

static void destroy_pagelist(DB_ENV *dbenv, MPRO_PAGE_LIST *pagelist) {
    // Pthread_mutex_destroy(&pagelist->lk);
    __os_free(dbenv, pagelist);
}

// This creates the header of a pagelist only.  Note that this is called with the mpro lock held.
static int create_pagelist_in_mpro(DBC *dbc, db_pgno_t pgno, MPRO_PAGE_LIST **pages) {
    DB *dbp = dbc->dbp;
    DB_ENV *dbenv = dbp->dbenv;
    MPRO_PAGE_LIST *pagelist = NULL;
    DB_MPRO *mpro = dbenv->mpro;
    auto
    int ret;

    ret = __os_malloc(dbenv, sizeof(MPRO_PAGE_LIST), &pagelist);
    if (ret)
        goto err;
    pagelist->key.pgno = pgno;
    memcpy(pagelist->key.ufid, dbc->dbp->mpf->fileid, DB_FILE_ID_LEN);
    listc_init(&pagelist->pages, offsetof(MPRO_PAGE_HEADER, commit_order));
    *pages = pagelist;
    hash_add(mpro->pages, pagelist);

    return 0;
err:
    return ret;
}

static char* name_page(DB_ENV *dbenv, MPRO_PAGE_LIST *pagelist, char *out) {
    char *fname;
    char ufid[DB_FILE_ID_LEN * 2 + 1];
    util_tohex(ufid, (char*) pagelist->key.ufid, DB_FILE_ID_LEN);
    int ret = __ufid_to_fname(dbenv, &fname, pagelist->key.ufid);
    sprintf(out, "%s pgno %u", ret == 0 ? fname : ufid, pagelist->key.pgno);
    return out;
}

static void mpro_panic(DB_ENV *dbenv) {
    DB_MPRO *mpro = dbenv->mpro;
    char pgname[100];

    printf("pagelist hash:\n");
    MPRO_PAGE_LIST *pagelist;
    unsigned int bkt;
    void *hashpos;
    int i = 0;
    for (pagelist = (MPRO_PAGE_LIST*) hash_first(mpro->pages, &hashpos, &bkt); pagelist;
         pagelist = (MPRO_PAGE_LIST*)hash_next(mpro->pages, &hashpos, &bkt)) {
        printf("%d: %s (%d pages):\n", i++, name_page(dbenv, pagelist, pgname), pagelist->pages.count);
        MPRO_PAGE_HEADER *hdr;
        LISTC_FOR_EACH(&pagelist->pages, hdr, commit_order) {
            PAGE *p = (PAGE*) hdr->page;
            printf("  %u:%u %"PRId64" pin %hd\n", p->lsn.file, p->lsn.offset, TXNID(p), hdr->pin);
        }
    }
    __db_panic(dbenv, ENOMEM);
}

/*
 * PUBLIC: int __mempro_add_txn __P((DB_ENV *dbenv, u_int64_t txnid, DB_LSN commit_lsn));
 */
int __mempro_add_txn(DB_ENV *dbenv, u_int64_t txnid, DB_LSN commit_lsn) {
    UTXNID_TRACK *txn, *old;

    if (IS_ZERO_LSN(commit_lsn))
        return 0;

    time_t now = time(NULL);
    int ret = __os_malloc(dbenv, sizeof(UTXNID_TRACK), &txn);
    if (ret)
        return ENOMEM;
    txn->txnid = txnid;
    txn->commit_lsn = commit_lsn;
    txn->timestamp = now;

    // printf("%"PRIx64" %u:%u\n", txnid, commit_lsn.file, commit_lsn.offset);

    Pthread_mutex_lock(&dbenv->mpro->mpro_mutexp);
    old = hash_find(dbenv->mpro->transactions, &txnid);
    // we can have duplicates if we truncate and come up with the same LSN again
    if (old) {
        listc_rfl(&dbenv->mpro->translist, old);
        hash_del(dbenv->mpro->transactions, old);
        __os_free(dbenv, old);
    }
    hash_add(dbenv->mpro->transactions, txn);
    listc_atl(&dbenv->mpro->translist, txn);
    // TODO: configurable time
    old = dbenv->mpro->translist.bot;
    while (old && old->timestamp - now > 600) {
        hash_del(dbenv->mpro->transactions, old);
        listc_rbl(&dbenv->mpro->translist);
        __os_free(dbenv, old);
        old = dbenv->mpro->translist.bot;
    }
    Pthread_mutex_unlock(&dbenv->mpro->mpro_mutexp);

#ifndef NDEBUG
    {
        DB_LSN chklsn;
        int chkret = __mempro_get_commit_lsn_for_txn(dbenv, txnid, &chklsn);
        if (chkret != 0) {
            __mempro_get_commit_lsn_for_txn(dbenv, txnid, &chklsn);
        }
        assert(chkret == 0 && log_compare(&chklsn, &commit_lsn) == 0);
    }
#endif

    return 0;
}
/*
 * PUBLIC: int __mempro_get_commit_lsn_for_txn __P((DB_ENV *dbenv, u_int64_t txnid, DB_LSN *commit_lsn));
 */
int __mempro_get_commit_lsn_for_txn(DB_ENV *dbenv, u_int64_t txnid, DB_LSN *commit_lsn) {
    UTXNID_TRACK *txn;
    DB_MPRO *mpro = dbenv->mpro;
    int ret = 0;
    ZERO_LSN(*commit_lsn);

//    Pthread_mutex_lock(&mpro->mpro_mutexp);
    txn = hash_find(mpro->transactions, &txnid);
    if (txn == NULL)
        ret = DB_NOTFOUND;
    else
        *commit_lsn = txn->commit_lsn;
//    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    return ret;
}

void *gbl_mpro_base = NULL;

/*
 * PUBLIC: int __mempro_init __P((DB_ENV *dbenv, u_int64_t size));
 */
int __mempro_init (DB_ENV *dbenv, u_int64_t size) {
    int ret;
    DB_MPRO *mp;

    ret = __os_calloc(dbenv, 1, sizeof(DB_MPRO), &mp);
    if (ret)
        goto err;
    mp->pages = hash_init_o(offsetof(MPRO_PAGE_LIST, key), sizeof(MPRO_KEY));
    if (mp->pages == NULL) {
        ret = ENOMEM;
        goto err;
    }
    mp->transactions = hash_init_o(offsetof(UTXNID_TRACK, txnid), sizeof(u_int64_t));
    if (mp->transactions == NULL) {
        ret = ENOMEM;
        goto err;
    }
    gbl_mpro_base = malloc(size);
    if (gbl_mpro_base == NULL) {
        ret = ENOMEM;
        goto err;
    }
    mp->msp = create_mspace_with_base(gbl_mpro_base, size, 1);
    mp->size = size;
    dbenv->mpro = mp;
    Pthread_mutex_init(&mp->mpro_mutexp, NULL);
    listc_init(&mp->pagelru, offsetof(MPRO_PAGE_HEADER, lrulnk));
    listc_init(&mp->translist, offsetof(UTXNID_TRACK, lnk));
    return 0;

err:
    return ret;
}

/*
 * PUBLIC: int __mempro_destroy __P((DB_ENV *env));
 */
int __mempro_destroy(DB_ENV *env) {
    Pthread_mutex_destroy(&env->mpro->mpro_mutexp);
    __os_free(env, env->mpro);
    return 0;
}

/*
 * PUBLIC: int __dbenv_last_commit_lsn __P((DB_ENV *dbenv, DB_LSN *lsnp));
 */
int __dbenv_last_commit_lsn(DB_ENV *dbenv, DB_LSN *lsnp) {
    DB_MPRO *mpro = dbenv->mpro;
    Pthread_mutex_lock(&mpro->mpro_mutexp);
    if (mpro->translist.top)
        *lsnp = mpro->translist.top->commit_lsn;
    else {
        // TODO: we can have no transactions on the list - consider an idle database.
        ZERO_LSN(*lsnp);
    }
    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    return 0;
}

/*
 * PUBLIC: int __mempro_fput __P((DBC *dbc, void *page));
 */
int __mempro_fput(DBC *dbc, void *page) {
    DB_ENV *dbenv = dbc->dbp->dbenv;
    DB_MPRO *mpro = dbenv->mpro;
    MPRO_PAGE_HEADER *hdr = (MPRO_PAGE_HEADER *) ((uintptr_t)page - offsetof(MPRO_PAGE_HEADER, page));

    Pthread_mutex_lock(&mpro->mpro_mutexp);
    hdr->pin--;
    if (hdr->pin == 0) {
        listc_abl(&mpro->pagelru, hdr);
    }
    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    return 0;
}

int dumputxn(void *obj, void *arg) {
    (void)arg;
    UTXNID_TRACK *t = (UTXNID_TRACK*) obj;
    printf("   %"PRId64 " " PR_LSN"\n", t->txnid, PARM_LSN(t->commit_lsn));
    return 0;
}

// We have some newer version of a page that we found on the side of the road somewhere. The only guarantee we have
// is that version is newer than what we want to see.  Roll it back to a version that's <= our start LSN.
// Our start LSN is the commit LSN of the latest transaction to commit when we start.  That's passed to us as
// want_txnid.
static int get_page_at_txnid(DB_ENV *dbenv, PAGE *pg, DB_LSN *want_lsn) {
    // walk the log for the page, undo changes until we get to a page
    // modified by a transasction who's txnid <= our txnid
    DB_LOGC *logc = NULL;
    int ret;
    DBT dbt = {0};
    u_int64_t pgtxnid;
    DB_MPRO *mpro = dbenv->mpro;

    DB_LSN current_txn_lsn;
    if (DEBUG_PAGES)
        printf("  %s pgno %d to LSN %u:%u\n", __func__, PGNO(pg), want_lsn->file, want_lsn->offset);

    // If the page format we got has no LSN logged, we have nothing to go back to
    if (IS_NOT_LOGGED_LSN(LSN(pg))) {
        if (DEBUG_PAGES)
            printf("    page not logged, returning as is\n");
        return 0;
    }

    if ((ret = __log_cursor(dbenv, &logc)) != 0) {
        if (DEBUG_PAGES)
            printf("    can't get log cursor\n");
        goto err;
    }
    dbt.flags = DB_DBT_REALLOC;
    ret = logc->get(logc, &LSN(pg), &dbt, DB_SET);
    if (ret) {
        if (DEBUG_PAGES)
            printf("    can't position log cursor at "PR_LSN" rc %d\n", PARM_LSN(LSN(pg)), ret);
        goto err;
    }
    int found = 0;
    DB_LSN next;
    DB_LSN prevlsn;
    pgtxnid = TXNID(pg);
    ret = __mempro_get_commit_lsn_for_txn(dbenv, pgtxnid, &current_txn_lsn);
    // if we can't get this transaction, then the
    // TODO: recovery needs to collect txnids for all the transactions whose updates we may be asked to unroll
    if (ret) {
        // it's ok if we can't find a transaction - it's deemed to old for us to care about and we move on
        ret = 0;
        goto err;
    }
    if (ret) {
        printf("can't find commit LSN for txnid %"PRId64"\n", pgtxnid);
        UTXNID_TRACK *t;
        printf("list:\n");
        LISTC_FOR_EACH(&mpro->translist, t, lnk) {
            printf("   %"PRId64 " " PR_LSN"\n", t->txnid, PARM_LSN(t->commit_lsn));
        }
        printf("hash:\n");
        hash_for(mpro->transactions, dumputxn, NULL);
        abort();
        goto err;
    }
    while (log_compare(&current_txn_lsn, want_lsn) > 0) {
        u_int32_t rectype;
        void *args;
        if (dbt.size < sizeof(int)) {
            __db_err(dbenv, "Unexpected record size %ud\n", (int) dbt.size);
            ret = EINVAL;
            goto err;
        }
        args = NULL;
        LOGCOPY_32(&rectype, dbt.data);
        __db_addrem_args *addrem_args;
        __bam_cdel_args *cdel_args;
        __bam_repl_args *repl_args;
        int (*apply)(DB_ENV*, DBT*, DB_LSN*, db_recops, void *);
        // TODO: have a routine that does this, and pass enough information to recover routines so they don't
        //       have to read/decode again.
        // If it's a rectype that uses ufid instead of a fileid, handle it like it's base record type.
        if (rectype > DB_ufid_BEGIN  && rectype < DB_user_BEGIN)
            rectype -= DB_ufid_BEGIN;
        printf("    undo %u:%u rectype %d\n", current_txn_lsn.file, current_txn_lsn.offset, rectype);
        switch (rectype) {
            case DB___db_addrem: {
                ret = __db_addrem_read(dbenv, dbt.data, &addrem_args);
                next = addrem_args->prev_lsn;
                args = addrem_args;
                apply = __db_addrem_recover;
                break;
            }
            case DB___bam_cdel: {
                ret = __bam_cdel_read(dbenv, dbt.data, &cdel_args);
                next = cdel_args->prev_lsn;
                args = cdel_args;
                apply = __bam_cdel_recover;
                break;
            }
            case DB___bam_repl: {
                ret = __bam_repl_read(dbenv, dbt.data, &repl_args);
                next = repl_args->prev_lsn;
                args = repl_args;
                apply = __bam_repl_recover;
                break;
            }
            default:
                __db_err(dbenv, "Don't know how to handle rectype %u\n", rectype);
                ret = EINVAL;
                goto err;
        }
        if (args)
            free(args);
        // ret from record read
        if (ret) {
            if (DEBUG_PAGES)
                printf("    can't read log record at "PR_LSN"\n",PARM_LSN(LSN(pg)));
            goto err;
        }
        ret = apply(dbenv, &dbt, &prevlsn, DB_TXN_ABORT, pg);
        if (ret) {
            if (DEBUG_PAGES)
                printf("    can't rollback log record at "PR_LSN" ret %d\n",PARM_LSN(LSN(pg)), ret);
            goto err;
        }

        // The chain can end if we get to an LSN that's not logged, or to a page that never had a previous modification
        if (IS_ZERO_LSN(next) || IS_NOT_LOGGED_LSN(next))
            break;

        ret = logc->get(logc, &next, &dbt, DB_SET);
        if (ret) {
            if (DEBUG_PAGES)
                printf("    can't read previous record at "PR_LSN"\n", PARM_LSN(next));
            goto err;
        }
        pgtxnid = TXNID(pg);
        ret = __mempro_get_commit_lsn_for_txn(dbenv, pgtxnid, &current_txn_lsn);
        if (ret) {
            ret = 0;
            goto err;
            if (DEBUG_PAGES)
                printf("    can't find commit LSN for previous txnid %"PRId64"\n", pgtxnid);
            goto err;
        }
    }

err:
    if (logc)
        logc->close(logc, 0);
    if (dbt.data)
        free(dbt.data);
    return ret;
}

/*
 * PUBLIC: int __mpro_dump __P((DB_ENV *dbenv));
 */
int __mpro_dump(DB_ENV *dbenv) {
    DB_MPRO *mpro = dbenv->mpro;
    if (mpro == NULL)
        return ENOENT;
    return 0;
}
