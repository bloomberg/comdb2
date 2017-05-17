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

#ifndef __llog_handlers_h__
#define __llog_handlers_h__

#include "llog_auto.h"

int handle_undo_add_dta(DB_ENV *dbenv, u_int32_t rectype,
                        llog_undo_add_dta_args *addop, DB_LSN *lsn,
                        db_recops op);

int handle_undo_add_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_add_ix_args *addop, DB_LSN *lsn, db_recops op);

int handle_undo_del_dta(DB_ENV *dbenv, u_int32_t rectype,
                        llog_undo_del_dta_args *delop, DB_LSN *lsn,
                        db_recops op);

int handle_undo_del_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_del_ix_args *delop, DB_LSN *lsn, db_recops op);

int handle_undo_upd_dta(DB_ENV *dbenv, u_int32_t rectype,
                        llog_undo_upd_dta_args *updop, DB_LSN *lsn,
                        db_recops op);

int handle_undo_upd_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_upd_ix_args *delop, DB_LSN *lsn, db_recops op);

int handle_scdone(DB_ENV *dbenv, u_int32_t rectype, llog_scdone_args *scdoneop,
                  DB_LSN *lsn, db_recops op);

int handle_commit(DB_ENV *dbenv, u_int32_t rectype,
                  llog_ltran_commit_args *args, DB_LSN *lsn,
                  unsigned long long *commit_genid, db_recops op);

int handle_start(DB_ENV *dbenv, u_int32_t rectype, llog_ltran_start_args *args,
                 DB_LSN *lsn, db_recops op);

int handle_comprec(DB_ENV *dbenv, u_int32_t rectype,
                   llog_ltran_comprec_args *args, DB_LSN *lsn, db_recops op);

int handle_undo_add_dta_lk(DB_ENV *dbenv, u_int32_t rectype,
                           llog_undo_add_dta_lk_args *addop, DB_LSN *lsn,
                           db_recops op);

int handle_undo_add_ix_lk(DB_ENV *dbenv, u_int32_t rectype,
                          llog_undo_add_ix_lk_args *addop, DB_LSN *lsn,
                          db_recops op);

int handle_undo_del_dta_lk(DB_ENV *dbenv, u_int32_t rectype,
                           llog_undo_del_dta_lk_args *delop, DB_LSN *lsn,
                           db_recops op);

int handle_undo_del_ix_lk(DB_ENV *dbenv, u_int32_t rectype,
                          llog_undo_del_ix_lk_args *delop, DB_LSN *lsn,
                          db_recops op);

int handle_undo_upd_dta_lk(DB_ENV *dbenv, u_int32_t rectype,
                           llog_undo_upd_dta_lk_args *updop, DB_LSN *lsn,
                           db_recops op);

int handle_undo_upd_ix_lk(DB_ENV *dbenv, u_int32_t rectype,
                          llog_undo_upd_ix_lk_args *delop, DB_LSN *lsn,
                          db_recops op);

int handle_rowlocks_log_bench(DB_ENV *dbenv, u_int32_t rectype,
                              llog_rowlocks_log_bench_args *rl_log_bench,
                              DB_LSN *lsn, db_recops op);

int handle_commit_log_bench(DB_ENV *dbenv, u_int32_t rectype,
                            llog_commit_log_bench_args *c_log_bench,
                            DB_LSN *lsn, db_recops op);

#endif /* __llog_handlers_h__ */
