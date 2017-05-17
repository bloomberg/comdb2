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

#ifndef INCLUDED_MISSING_H
#define INCLUDED_MISSING_H

void berkdb_set_recovery(DB_ENV *);

int __lock_locker_haslocks(DB_ENV *, u_int32_t lockerid);

int llog_undo_add_dta_log(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                          u_int32_t flags, const DBT *table, short dtafile,
                          short dtastripe, u_int64_t genid, u_int64_t ltranid,
                          DB_LSN *prevllsn, const DBT *lock);

int llog_undo_del_dta_log(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                          u_int32_t flags, const DBT *table, u_int64_t genid,
                          u_int64_t ltranid, DB_LSN *prevllsn, short dtafile,
                          short dtastripe, int dtalen, const DBT *lock);

int llog_undo_del_ix_log(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                         u_int32_t flags, const DBT *table, u_int64_t genid,
                         short ix, u_int64_t ltranid, DB_LSN *prevllsn,
                         const DBT *keylock, int keylen, int dtalen);

int llog_undo_upd_ix_log(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                         u_int32_t flags, const DBT *table, u_int64_t oldgenid,
                         u_int64_t newgenid, u_int64_t ltranid,
                         DB_LSN *prevllsn, short ix, const DBT *key,
                         int dtalen);

int llog_undo_add_ix_log(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                         u_int32_t flags, const DBT *table, short ix,
                         u_int64_t genid, u_int64_t ltranid, DB_LSN *prevllsn,
                         int keylen, int dtalen);

int llog_undo_upd_dta_log(DB_ENV *dbenv, DB_TXN *txnid, DB_LSN *ret_lsnp,
                          u_int32_t flags, const DBT *table, u_int64_t oldgenid,
                          u_int64_t newgenid, u_int64_t ltranid,
                          DB_LSN *prevllsn, short dtafile, short dtastripe,
                          const DBT *lockold, const DBT *locknew,
                          int old_dta_len);
#endif
