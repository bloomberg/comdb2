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

int create_pagelist_in_mpro(DBC *dbc, db_pgno_t pgno, MPRO_PAGE_LIST **pages);

int find_page_for_cursor(MPRO_PAGE_LIST *pagelist, DBC *dbc, db_pgno_t pgno, PAGE **page);

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
    *(void**)page = h;
    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    return 0;

err:
    // There's no separate lru for page lists - we remove the page list if there's no more pages.
    // If we created a page list but failed to add a page to it, remove it here.
    if (nopages && pages && pages->pages.count == 0) {
        Pthread_mutex_lock(&mpro->mpro_mutexp);
        hash_del(mpro->pages, pages);
        Pthread_mutex_unlock(&mpro->mpro_mutexp);
        free(pages);
    }
    abort();
    return ret;
}

// Find a page image at the right LSN.  This should be called with the mpro lock released and a pagelist lock held
int find_page_for_cursor(MPRO_PAGE_LIST *pagelist, DBC *dbc, db_pgno_t pgno, PAGE **page) {
    DB *dbp = dbc->dbp;
    DB_ENV *dbenv = dbp->dbenv;
    DB_MPOOLFILE *mpf = dbp->mpf;
    MPRO_PAGE_HEADER *hdr;
    DB_MPRO *mpro = dbenv->mpro;
    DB_LOCK_ILOCK lk;
    DB_LOCK lock;
    PAGE *h;

    // This is the txnid we're targetting
    u_int64_t utxnid = dbc->utxnid;

    int have_lock = 0, have_page = 0, have_page_image = 0;
    int ret;

    // Walk a list of any cached versions of this page we have.
    hdr = pagelist->pages.top;
    while (hdr) {
        h = (PAGE*) hdr->page;
        if (h->txnid <= utxnid) {
            // We found a page that's older than our transaction. Now we need to check that there's no newer
            // transaction that also modified the page.  If there isn't, this is the right version.  The check is easy
            // if we have a newer transaction in the list preceding us - just check that it's previous id is us.  If
            // it is, we're done.  If not, there may be transactions that modified this page between us and the version
            // in the list.  The previous page is newer than what we need (or we would have stopped there). So start
            // there and roll back to the version we need. If there's no previous version, we need to start somewhere
            // so grab it from the "real" buffer pool.
            if (hdr->commit_order.prev && ((PAGE*) hdr->commit_order.prev->page)->prev_txnid == h->txnid) {
                if (hdr->pin == 0) {
                    listc_rfl(&mpro->pagelru, hdr);
                }
                hdr->pin++;
                *page = (PAGE*) hdr->page;
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
            MPRO_PAGE_HEADER *lruhdr;
            lruhdr = listc_rtl(&dbenv->mpro->pagelru);
            if (lruhdr == NULL) {
                __db_panic(dbenv, ENOMEM);
                ret = ENOMEM;
                goto err;
            }
            hash_del(mpro->pages, lruhdr);
            mspace_free(mpro->msp, lruhdr);
        }
    } while (hdr == NULL);
    hdr->pin = 0;
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
            goto err;
        }
        else if (ret) {
            goto err;
        }
        have_lock = 1;

        ret = __memp_fget(mpf, &pgno, 0, &h);
        if (ret) {
            goto err;
        }
        have_page = 1;
        ret = __lock_put(dbenv, &lock);
        if (ret) {
            goto err;
        }
        have_lock = 0;
        ret = __memp_fput(mpf, h, 0);
        if (ret) {
            goto err;
        }
        have_page = 0;
        memcpy(hdr->page, h, dbp->pgsize);
    }
    // We now have a newer but still wrong page image.  Recover to the version we want

    *page = (PAGE*) hdr->page;
    return 0;
err:
    return ret;
}

// This creates the header of a pagelist only.  Note that this is called with the mpro lock held.
int create_pagelist_in_mpro(DBC *dbc, db_pgno_t pgno, MPRO_PAGE_LIST **pages) {
    DB *dbp = dbc->dbp;
    DB_ENV *dbenv = dbp->dbenv;
    MPRO_PAGE_LIST *pagelist = NULL;
    DB_MPRO *mpro = dbenv->mpro;
    auto
    int ret;

    ret = __os_malloc(dbenv, sizeof(MPRO_PAGE_LIST), &pagelist);
    if (ret)
        goto err;
    *pages = pagelist;
    listc_init(&pagelist->pages, offsetof(MPRO_PAGE_HEADER, commit_order));
    hash_add(mpro->pages, pagelist);

    return 0;
err:
    return ret;
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

    printf("%"PRIx64" %u:%u\n", txnid, commit_lsn.file, commit_lsn.offset);

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

    Pthread_mutex_lock(&mpro->mpro_mutexp);
    txn = hash_find(mpro->transactions, &txnid);
    if (txn == NULL)
        ret = DB_NOTFOUND;
    else
        *commit_lsn = txn->commit_lsn;
    Pthread_mutex_unlock(&mpro->mpro_mutexp);
    return ret;
}

void *gbl_mpro_base = NULL;

/*
 * PUBLIC: int __mempro_init __P((DB_ENV *env, u_int64_t size));
 */
int __mempro_init (DB_ENV *env, u_int64_t size) {
    int ret;
    DB_MPRO *mp;

    ret = __os_calloc(env, 1, sizeof(DB_MPRO), &mp);
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
    env->mpro = mp;
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
 * PUBLIC: int __dbenv_last_commit_lsn __P((DB_ENV *dbenv, u_int64_t *utxnid));
 */
int __dbenv_last_commit_lsn(DB_ENV *dbenv, u_int64_t *utxnid) {
    DB_MPRO *mpro = dbenv->mpro;
    Pthread_mutex_lock(&mpro->mpro_mutexp);
    if (mpro->translist.top)
        *utxnid = mpro->translist.top->txnid;
    else
        *utxnid = 0;
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

// We have some newer version of a page that we found on the side of the road somewhere. The only guarantee we have
// is that version is newer than what we want to see.  Roll it back to a version that's <= our start LSN.
// Our start LSN is the commit LSN of the latest transaction to commit when we start.
/*
 * PUBLIC: int __mempro_get_page_as_of_lsn __P((DB_ENV *dbenv, PAGE *p, u_int64_t want_txnid));
 */
int __mempro_get_page_as_of_lsn(DB_ENV *dbenv, PAGE *pg, u_int64_t want_txnid) {
    // walk the for the page, undo changes until we get to a page
    // modified by a transasction who's commit LSN <= our LSN
    DB_LOGC *logc = NULL;
    int ret;
    DBT dbt = {0};
    u_int64_t pgtxnid;

    if ((ret = __log_cursor(dbenv, &logc)) != 0)
        goto err;
    dbt.flags = DB_DBT_REALLOC;
    ret = logc->get(logc, &LSN(pg), &dbt, DB_SET);
    if (ret)
        goto err;
    int found = 0;
    DB_LSN next;
    DB_LSN prevlsn;
    pgtxnid = TXNID(pg);
    while (pgtxnid > want_txnid) {
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
                __db_err(dbenv, "Don't know how to handle rectype %ud\n", rectype);
                ret = EINVAL;
                goto err;
        }
        if (args)
            free(args);
        // ret from record read
        if (ret)
            goto err;
        ret = apply(dbenv, &dbt, &prevlsn, DB_TXN_ABORT, pg);
        if (ret)
            goto err;

        ret = logc->get(logc, &next, &dbt, DB_SET);
        if (ret)
            goto err;
        pgtxnid = TXNID(pg);
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