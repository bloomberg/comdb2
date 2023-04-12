/* DO NOT EDIT: automatically built by dist/s_include. */
#ifndef	_txn_ext_h_
#define	_txn_ext_h_

#if defined(__cplusplus)
extern "C" {
#endif

int __txn_begin_pp __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t));
int __txn_assert_notran_pp __P((DB_ENV *));
int __txn_begin_with_prop_pp __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t,
                                       struct txn_properties *));
int __txn_begin __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t));
int __txn_begin_main __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t, struct txn_properties *));
int __txn_begin_with_prop __P((DB_ENV *, DB_TXN *, DB_TXN **, u_int32_t, struct txn_properties *));
int __txn_xa_begin __P((DB_ENV *, DB_TXN *));
int __txn_compensate_begin __P((DB_ENV *, DB_TXN **txnp));
int __txn_commit __P((DB_TXN *, u_int32_t));
int __txn_abort __P((DB_TXN *));
int __txn_discard __P((DB_TXN *, u_int32_t flags));
int __txn_prepare __P((DB_TXN *, u_int8_t *));
u_int32_t __txn_id __P((DB_TXN *));
int  __txn_set_timeout __P((DB_TXN *, db_timeout_t, u_int32_t));
int __txn_checkpoint_pp __P((DB_ENV *, u_int32_t, u_int32_t, u_int32_t));
int __txn_checkpoint __P((DB_ENV *, u_int32_t, u_int32_t, u_int32_t));
int __txn_getckp __P((DB_ENV *, DB_LSN *));
int __txn_activekids __P((DB_ENV *, u_int32_t, DB_TXN *));
int __txn_force_abort __P((DB_ENV *, u_int8_t *));
int __txn_preclose __P((DB_ENV *));
int __txn_reset __P((DB_ENV *));
void __txn_updateckp __P((DB_ENV *, DB_LSN *));
int __txn_regop_gen_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int64_t *, u_int32_t,  u_int32_t, u_int32_t, u_int64_t, const DBT *, void *));
int __txn_regop_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int64_t *, u_int32_t, u_int32_t, int32_t, const DBT *));
int __txn_regop_getpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_print __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_gen_read __P((DB_ENV *, void *, __txn_regop_gen_args **));
int __txn_regop_read __P((DB_ENV *, void *, __txn_regop_args **));
unsigned long long __txn_regop_read_context __P((__txn_regop_args *));
int __txn_regop_rowlocks_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int64_t *, u_int32_t, u_int32_t, u_int64_t, DB_LSN *, DB_LSN *,u_int64_t, u_int32_t, u_int32_t, const DBT *, const DBT *, void *));
int __txn_regop_rowlocks_getpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_rowlocks_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_rowlocks_print __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_rowlocks_read __P((DB_ENV *, void *, __txn_regop_rowlocks_args **));
int __txn_ckp_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int32_t, DB_LSN *, DB_LSN *, int32_t, u_int32_t, u_int64_t));
int __txn_ckp_getpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_ckp_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_ckp_print __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_ckp_read __P((DB_ENV *, void *, __txn_ckp_args **));
int __txn_child_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int32_t, u_int32_t, u_int64_t, DB_LSN *));
int __txn_child_getpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_child_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_child_print __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_child_read __P((DB_ENV *, void *, __txn_child_args **));
int __txn_xa_regop_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int32_t, u_int32_t, const DBT *, int32_t, u_int32_t, u_int32_t, DB_LSN *, const DBT *));
int __txn_xa_regop_getpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_xa_regop_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_xa_regop_print __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_xa_regop_read __P((DB_ENV *, void *, __txn_xa_regop_args **));
int __txn_recycle_log __P((DB_ENV *, DB_TXN *, DB_LSN *, u_int32_t, u_int32_t, u_int32_t));
int __txn_recycle_getpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_recycle_getallpgnos __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_recycle_print __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_recycle_read __P((DB_ENV *, void *, __txn_recycle_args **));
int __txn_init_print __P((DB_ENV *, int (***)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *), size_t *));
int __txn_init_getpgnos __P((DB_ENV *, int (***)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *), size_t *));
int __txn_init_getallpgnos __P((DB_ENV *, int (***)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *), size_t *));
int __txn_init_recover __P((DB_ENV *, int (***)(DB_ENV *, DBT *, DB_LSN *, db_recops, void *), size_t *));
DB_LSN __txn_get_first_dirty_begin_lsn __P((DB_LSN));
void __txn_dbenv_create __P((DB_ENV *));
int __txn_set_tx_max __P((DB_ENV *, u_int32_t));
int __txn_regop_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_gen_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_regop_rowlocks_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_xa_regop_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_ckp_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_child_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
int __txn_restore_txn __P((DB_ENV *, DB_LSN *, __txn_xa_regop_args *));
int __txn_recycle_recover __P((DB_ENV *, DBT *, DB_LSN *, db_recops, void *));
void __txn_continue __P((DB_ENV *, DB_TXN *, TXN_DETAIL *, size_t));
int __txn_map_gid __P((DB_ENV *, u_int8_t *, TXN_DETAIL **, size_t *));
int __txn_recover_pp __P((DB_ENV *, DB_PREPLIST *, long, long *, u_int32_t));
int __txn_recover __P((DB_ENV *, DB_PREPLIST *, long, long *, u_int32_t));
int __txn_get_prepared __P((DB_ENV *, XID *, DB_PREPLIST *, long, long *, u_int32_t));
int __txn_open __P((DB_ENV *));
int __txn_dbenv_refresh __P((DB_ENV *));
void __txn_region_destroy __P((DB_ENV *, REGINFO *));
int __txn_id_set __P((DB_ENV *, u_int32_t, u_int32_t));
int __txn_stat_pp __P((DB_ENV *, DB_TXN_STAT **, u_int32_t));
int __txn_closeevent __P((DB_ENV *, DB_TXN *, DB *));
int __txn_remevent __P((DB_ENV *, DB_TXN *, const char *, u_int8_t*));
void __txn_remrem __P((DB_ENV *, DB_TXN *, const char *));
int __txn_lockevent __P((DB_ENV *, DB_TXN *, DB *, DB_LOCK *, u_int32_t));
void __txn_remlock __P((DB_ENV *, DB_TXN *, DB_LOCK *, u_int32_t));
int __txn_doevents __P((DB_ENV *, DB_TXN *, int, int));
int __txn_snapshot __P((DB_TXN *));
int __txn_allocate_ltrans __P((DB_ENV *, unsigned long long, DB_LSN *, LTDESC **));
int __txn_ltrans_find_lowest_lsn __P((DB_ENV *, DB_LSN *));
int __txn_count_ltrans __P((DB_ENV *, u_int32_t *));
int __txn_get_ltran_list __P((DB_ENV *, DB_LTRAN **, u_int32_t *));
int __txn_find_ltrans __P((DB_ENV *, unsigned long long, LTDESC **));
void __txn_deallocate_ltrans __P((DB_ENV *, LTDESC *));
int __txn_add_locklist_to_ltrans __P((DB_ENV *, LTDESC *, DB_LSN *, void *, 
            u_int32_t, u_int32_t));

#if defined(__cplusplus)
}
#endif
#endif /* !_txn_ext_h_ */
