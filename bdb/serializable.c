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

/* now used for serializable transaction read-set validation --Lingzhi Deng */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <pthread.h>
#include <assert.h>
#include <strings.h>

#include <list.h>
#include <fsnap.h>

#include <bdb_osqllog.h>
#include <bdb_osqltrn.h>
#include <bdb_int.h>
#include <bdb_osqlcur.h>
#include <flibc.h>

#include <build/db.h>
#include <build/db_int.h>
#include <dbinc_auto/db_auto.h>
#include <dbinc_auto/dbreg_auto.h>
#include <dbinc/txn.h>
#include <dbinc_auto/txn_auto.h>
#include <dbinc_auto/txn_ext.h>
#include <dbinc/btree.h>
#include <dbinc/db_page.h>
#include <dbinc/db_shash.h>
#include <dbinc/db_swap.h>
#include <dbinc/lock.h>
#include <dbinc/mp.h>

#include <llog_auto.h>
#include <llog_ext.h>

int serial_check_this_txn(bdb_state_type *bdb_state, DB_LSN lsn, void *ranges)
{
    int rc = 0;
    DBT logdta;
    DB_LSN undolsn;
    DB_LOGC *cur = NULL;
    u_int32_t rectype = 0;
    void *key;

    llog_undo_add_dta_args *add_dta;
    llog_undo_add_ix_args *add_ix;
    llog_ltran_commit_args *commit;
    llog_ltran_comprec_args *comprec;
    llog_undo_del_dta_args *del_dta;
    llog_undo_del_ix_args *del_ix;
    llog_undo_upd_dta_args *upd_dta;
    llog_undo_upd_ix_args *upd_ix;

    /* Rowlocks types */
    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;
    void *logp = NULL;

    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;

    /* get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        printf("%s:%d log_cursor rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }
    rc = cur->get(cur, &lsn, &logdta, DB_SET);
    if (!rc)
        LOGCOPY_32(&rectype, logdta.data);
    else {
        fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n", rc);
        fprintf(stderr, "@ file: %d, offset %d\n", lsn.file, lsn.offset);
        return 1;
    }
    while (rc == 0 && rectype != DB_llog_ltran_start) {
        switch (rectype) {

        case DB_llog_undo_add_dta:
            rc =
                llog_undo_add_dta_read(bdb_state->dbenv, logdta.data, &add_dta);
            if (rc)
                return rc;
            logp = add_dta;
            rc = bdb_state->callback->serialcheck_rtn(add_dta->table.data, -2,
                                                      NULL, 0, ranges);
            lsn = add_dta->prevllsn;
            break;

        case DB_llog_undo_add_ix:
            rc = llog_undo_add_ix_read(bdb_state->dbenv, logdta.data, &add_ix);
            if (rc)
                return rc;
            logp = add_ix;
            key = malloc(add_ix->keylen);
            undolsn = add_ix->prev_lsn;
            rc = bdb_reconstruct_add(bdb_state, &undolsn, key, add_ix->keylen,
                                     NULL, add_ix->dtalen, NULL, NULL);
            rc = bdb_state->callback->serialcheck_rtn(
                add_ix->table.data, add_ix->ix, key, add_ix->keylen, ranges);
            free(key);
            lsn = add_ix->prevllsn;
            break;

        case DB_llog_ltran_commit:
            rc = llog_ltran_commit_read(bdb_state->dbenv, logdta.data, &commit);
            if (rc)
                return rc;
            logp = commit;
            lsn = commit->prevllsn;
            break;

        case DB_llog_ltran_comprec:
            rc = llog_ltran_comprec_read(bdb_state->dbenv, logdta.data,
                                         &comprec);
            if (rc)
                return rc;
            logp = comprec;
            lsn = comprec->prevllsn;
            break;

        case DB_llog_ltran_start:
            /* no-op, except we need the previous LSN. and there can be no
               previous
               LSN since it's a start record. */
            logp = NULL;
            break;

        case DB_llog_undo_del_dta:
            rc =
                llog_undo_del_dta_read(bdb_state->dbenv, logdta.data, &del_dta);
            if (rc)
                return rc;
            logp = del_dta;
            rc = bdb_state->callback->serialcheck_rtn(del_dta->table.data, -2,
                                                      NULL, 0, ranges);
            lsn = del_dta->prevllsn;
            break;

        case DB_llog_undo_del_ix:
            rc = llog_undo_del_ix_read(bdb_state->dbenv, logdta.data, &del_ix);
            if (rc)
                return rc;
            logp = del_ix;
            key = malloc(del_ix->keylen);
            undolsn = del_ix->prev_lsn;
            rc = bdb_reconstruct_delete(bdb_state, &undolsn, NULL, NULL, key,
                                        del_ix->keylen, NULL, del_ix->dtalen,
                                        NULL);
            rc = bdb_state->callback->serialcheck_rtn(
                del_ix->table.data, del_ix->ix, key, del_ix->keylen, ranges);
            free(key);
            lsn = del_ix->prevllsn;
            break;

        case DB_llog_undo_upd_dta:
            rc =
                llog_undo_upd_dta_read(bdb_state->dbenv, logdta.data, &upd_dta);
            if (rc)
                return rc;
            logp = upd_dta;
            rc = bdb_state->callback->serialcheck_rtn(upd_dta->table.data, -2,
                                                      NULL, 0, ranges);
            lsn = upd_dta->prevllsn;
            break;

        case DB_llog_undo_upd_ix:
            rc = llog_undo_upd_ix_read(bdb_state->dbenv, logdta.data, &upd_ix);
            if (rc)
                return rc;
            logp = upd_ix;
            rc = bdb_state->callback->serialcheck_rtn(
                upd_ix->table.data, upd_ix->ix, upd_ix->key.data,
                upd_ix->key.size, ranges);
            lsn = upd_ix->prevllsn;
            break;

        case DB_llog_undo_add_dta_lk:
            rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &add_dta_lk);
            if (rc)
                return rc;
            logp = add_dta_lk;
            rc = bdb_state->callback->serialcheck_rtn(add_dta_lk->table.data,
                                                      -2, NULL, 0, ranges);
            lsn = add_dta_lk->prevllsn;
            break;

        case DB_llog_undo_add_ix_lk:
            rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &add_ix_lk);
            if (rc)
                return rc;
            logp = add_ix_lk;
            rc = bdb_state->callback->serialcheck_rtn(
                add_ix_lk->table.data, add_ix_lk->ix, add_ix_lk->key.data,
                add_ix_lk->key.size, ranges);
            lsn = add_ix_lk->prevllsn;
            break;

        case DB_llog_undo_del_dta_lk:
            rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &del_dta_lk);
            if (rc)
                return rc;
            logp = del_dta_lk;
            rc = bdb_state->callback->serialcheck_rtn(del_dta_lk->table.data,
                                                      -2, NULL, 0, ranges);
            lsn = del_dta_lk->prevllsn;
            break;

        case DB_llog_undo_del_ix_lk:
            rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &del_ix_lk);
            if (rc)
                return rc;
            logp = del_ix_lk;
            key = malloc(del_ix_lk->keylen);
            undolsn = del_ix_lk->prev_lsn;
            rc = bdb_reconstruct_delete(bdb_state, &undolsn, NULL, NULL, key,
                                        del_ix_lk->keylen, NULL,
                                        del_ix_lk->dtalen, NULL);
            rc = bdb_state->callback->serialcheck_rtn(
                del_ix_lk->table.data, del_ix_lk->ix, key, del_ix_lk->keylen,
                ranges);
            free(key);
            lsn = del_ix_lk->prevllsn;
            break;

        case DB_llog_undo_upd_dta_lk:
            rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &upd_dta_lk);
            if (rc)
                return rc;
            logp = upd_dta_lk;
            rc = bdb_state->callback->serialcheck_rtn(upd_dta_lk->table.data,
                                                      -2, NULL, 0, ranges);
            lsn = upd_dta_lk->prevllsn;
            break;

        case DB_llog_undo_upd_ix_lk:
            rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &upd_ix_lk);
            if (rc)
                return rc;
            logp = upd_ix_lk;
            rc = bdb_state->callback->serialcheck_rtn(
                upd_ix_lk->table.data, upd_ix_lk->ix, upd_ix_lk->key.data,
                upd_ix_lk->key.size, ranges);
            lsn = upd_ix_lk->prevllsn;
            break;

        default:
            fprintf(stderr, "Unknown log entry type %d\n", rectype);
            abort();
            rc = -1;
            break;
        }

        if (logdta.data) {
            free(logdta.data);
            logdta.data = NULL;
        }

        if (logp) {
            free(logp);
            logp = NULL;
        }

        if (rc) {
            /* one of the serialchecks failed */
            cur->close(cur, 0);
            return rc;
        }

        if (lsn.file == 0)
            break;

        /* Get previous logical log */
        rc = cur->get(cur, &lsn, &logdta, DB_SET);
        if (!rc)
            LOGCOPY_32(&rectype, logdta.data);
        else {
            fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n", rc);
            fprintf(stderr, "@ file: %d, offset %d\n", lsn.file, lsn.offset);
            return 1;
        }
    }

    if (logdta.data) {
        free(logdta.data);
        logdta.data = NULL;
    }

    if (logp) {
        free(logp);
        logp = NULL;
    }

    cur->close(cur, 0);
    return 0;
}

/*
 * Check the log, starting from the given lsn. against the read-set ranges
 * return non-zero if log file overlaps with given ranges (i.e. transaction
 * is not serializable), return 0 otherwise.
 * If the regop_only flag is set, return non-zero if a commit record
 * is found after the given lsn (ranges not checked).
*/
static int osql_serial_check(bdb_state_type *bdb_state, void *ranges,
                             unsigned int *file, unsigned int *offset,
                             int (*check_this_txn)(bdb_state_type *bdb_state,
                                                   DB_LSN lsn, void *ranges),
                             int regop_only)
{
    int rc;
    DBT logdta;
    DB_LOGC *cur = NULL;
    DB_LOGC *prevcur = NULL;
    DB_LSN seriallsn;
    DB_LSN prevlsn;
    DB_LSN curlsn;
    u_int32_t rectype = 0;
    __txn_regop_args *argp = NULL;
    __txn_regop_gen_args *arggenp = NULL;
    __txn_regop_rowlocks_args *argrlp = NULL;
    llog_ltran_commit_args *commit = NULL;

    seriallsn.file = *file;
    seriallsn.offset = *offset;

    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;

    /* get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        printf("%s:%d log_cursor rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }

    /* get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &prevcur, 0);
    if (rc) {
        printf("%s:%d log_cursor rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }

    /* Get the current lsn */
    __log_txn_lsn(bdb_state->dbenv, &curlsn, NULL, NULL);
    if (!regop_only) {
        *file = curlsn.file;
        *offset = curlsn.offset;
    }

    rc = 0;

    while (seriallsn.file < curlsn.file || seriallsn.offset <= curlsn.offset) {
        if (logdta.data) {
            free(logdta.data);
            logdta.data = NULL;
        }
        if (argp) {
            __os_free(bdb_state->dbenv, argp);
            argp = NULL;
        }
        if (argrlp) {
            free(argrlp);
            argrlp = NULL;
        }
        if (arggenp) {
            free(arggenp);
            arggenp = NULL;
        }
        if (commit) {
            free(commit);
            commit = NULL;
        }
        rc = cur->get(cur, &seriallsn, &logdta, DB_SET);
        if (!rc)
            LOGCOPY_32(&rectype, logdta.data);
        else if (rc == DB_NOTFOUND) {
            *file = seriallsn.file;
            *offset = seriallsn.offset;
            rc = 0;
            break;
        } else {
            fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n", rc);
            fprintf(stderr, "@ file: %d, offset %d\n", seriallsn.file,
                    seriallsn.offset);
            goto done;
        }

        /* find the next valid commit (prev_lsn of DB___txn_regop points to
         * DB_llog_ltran_commit) */
        while (1) {
            if (logdta.data) {
                free(logdta.data);
                logdta.data = NULL;
            }

            if (argp) {
                __os_free(bdb_state->dbenv, argp);
                argp = NULL;
            }
            if (argrlp) {
                free(argrlp);
                argrlp = NULL;
            }

            rc = cur->get(cur, &seriallsn, &logdta, DB_NEXT);
            if (!rc)
                LOGCOPY_32(&rectype, logdta.data);
            else if (rc == DB_NOTFOUND) {
                *file = seriallsn.file;
                *offset = seriallsn.offset;
                break;
            } else {
                fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n", rc);
                fprintf(stderr, "@ file: %d, offset %d\n", seriallsn.file,
                        seriallsn.offset);
                goto done;
            }
            if (rectype == DB___txn_regop) {
                rc = __txn_regop_read(bdb_state->dbenv, logdta.data, &argp);
                prevlsn = argp->prev_lsn;
                free(logdta.data);
                logdta.data = NULL;
                rc = prevcur->get(prevcur, &prevlsn, &logdta, DB_SET);
                if (!rc)
                    LOGCOPY_32(&rectype, logdta.data);
                else {
                    fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n",
                            rc);
                    fprintf(stderr, "@ file: %d, offset %d\n", seriallsn.file,
                            seriallsn.offset);
                    goto done;
                }
                if (rectype == DB_llog_ltran_commit) {
                    break;
                }
            } else if (rectype == DB___txn_regop_gen) {
                rc = __txn_regop_gen_read(bdb_state->dbenv, logdta.data,
                                          &arggenp);
                prevlsn = arggenp->prev_lsn;
                free(logdta.data);
                logdta.data = NULL;
                rc = prevcur->get(prevcur, &prevlsn, &logdta, DB_SET);
                if (!rc)
                    LOGCOPY_32(&rectype, logdta.data);
                else {
                    fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n",
                            rc);
                    fprintf(stderr, "@ file: %d, offset %d\n", seriallsn.file,
                            seriallsn.offset);
                    goto done;
                }
                if (rectype == DB_llog_ltran_commit) {
                    break;
                }
            } else if (rectype == DB___txn_regop_rowlocks) {
                rc = __txn_regop_rowlocks_read(bdb_state->dbenv, logdta.data,
                                               &argrlp);
                prevlsn = argrlp->prev_lsn;
                free(logdta.data);
                logdta.data = NULL;
                rc = prevcur->get(prevcur, &prevlsn, &logdta, DB_SET);
                if (!rc)
                    LOGCOPY_32(&rectype, logdta.data);
                else {
                    fprintf(stderr, "Unable to get last_logical_lsn, rc %d\n",
                            rc);
                    fprintf(stderr, "@ file: %d, offset %d\n", seriallsn.file,
                            seriallsn.offset);
                    goto done;
                }
                if (rectype == DB_llog_ltran_commit) {
                    break;
                }
            }
        }

        if (rc == DB_NOTFOUND) {
            /* not an error (EOF) */
            rc = 0;
            goto done;
        } else if (rc) {
            goto done;
        }

        if (logdta.data) {
            /* found a committed transaction */
            rc = llog_ltran_commit_read(bdb_state->dbenv, logdta.data, &commit);
            /* not a write transaction */
            if (commit->prevllsn.file == 0) {
                continue;
            }

            if (commit->isabort)
                continue;

            /* found a commited write transaction */
            if (!regop_only) {
                rc = check_this_txn(bdb_state, commit->prevllsn, ranges);
            } else
                rc = 1;
            if (rc) {
                goto done;
            }
        }
    }

done:
    if (logdta.data) {
        free(logdta.data);
        logdta.data = NULL;
    }
    if (argp) {
        __os_free(bdb_state->dbenv, argp);
        argp = NULL;
    }
    if (arggenp) {
        free(arggenp);
        arggenp = NULL;
    }
    if (argrlp) {
        free(argrlp);
        argrlp = NULL;
    }
    if (commit) {
        free(commit);
        commit = NULL;
    }
    /* close log cursor */
    cur->close(cur, 0);
    /* close log cursor */
    prevcur->close(prevcur, 0);

    /* return cursor get error or transaction failure */
    return rc;
}

int bdb_osql_serial_check(bdb_state_type *bdb_state, void *ranges,
                          unsigned int *file, unsigned int *offset,
                          int regop_only)
{
    if (!ranges)
        return 0;
    return osql_serial_check(bdb_state, ranges, file, offset,
                             serial_check_this_txn, regop_only);
}
