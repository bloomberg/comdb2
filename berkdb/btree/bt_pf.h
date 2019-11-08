#ifndef __BTPF__
#define __BTPF__

#define USE_BTPF 1

#define PF_ON 1
#define PF_OFF 0

#define RMBR_LVL 6

#define PFX(x) ((btpf*)x->pf)
#define BTPF_ENABLED(dbc) dbc->dbp->dbenv->attr.btpf_enabled

#define PG_GAP(dbc)   dbc->dbp->dbenv->attr.btpf_pg_gap
#define CU_GAP(dbc)   dbc->dbp->dbenv->attr.btpf_cu_gap;
#define WNDW_MIN(dbc) dbc->dbp->dbenv->attr.btpf_wndw_min
#define WNDW_INC(dbc) dbc->dbp->dbenv->attr.btpf_wndw_inc
#define WNDW_MAX(dbc) dbc->dbp->dbenv->attr.btpf_wndw_max
#define MIN_TH(dbc)   dbc->dbp->dbenv->attr.btpf_min_th

typedef enum {
	INIT,
	PF,
	STOPPED,
	LOADED_ALL
} btpf_status;			// not used yet

typedef enum {
	UNSET,
	FORWARD,
	BACKWARD
} btpf_direction;


typedef enum {
	SRCH_CUR,
	FIRST,
	LAST
} btfp_tw_flag;

typedef struct {
	u_int32_t reset;
	u_int32_t nxt_cnt;
	u_int32_t f_pf_cnt;
	u_int32_t b_pf_cnt;
	// ADD TIME CALCULATIONS (in the future)
} btpf_stats;

#define END_OF_TREE 1
#define DIFF_LSN 2

typedef struct {
	btpf_status  status; 
	btpf_direction direction;
	u_int32_t   rdr_rec_cnt; // records read in the same direction
	u_int32_t   rdr_pg_cnt; // pages read by the cursor to catch up
	u_int32_t   wndw;
	u_int32_t   on; // pre-faulting is on/off
   
	db_pgno_t curlf[RMBR_LVL];	// the chain of pages to reach the cursor 
	db_indx_t curindx[RMBR_LVL];	// current entry per level
	db_indx_t maxindx[RMBR_LVL];	// number of entry in each page
	DB_LSN lsn[RMBR_LVL];	// remember the lsn of when you found this information 

	db_pgno_t tr_page;

	btpf_stats stats;
} btpf;

typedef struct {
	DBT *rkey;
	DB *db;
	DB_MPOOLFILE *mpf;
	u_int32_t npages;
	u_int8_t dirty;
	btpf *pf;
	u_int32_t lid;
	u_int32_t locker;
} btpf_job;

btpf *btpf_init(void);
void btpf_free(btpf ** x);
void btpf_fprintf(FILE *f, btpf * x);

void btpf_rst(btpf * x);
void btpf_copy_dbc(DBC *dbc, DBC *ndbc);
void trk_descent(DB *dbp, DBC *dbc, PAGE *h, db_indx_t indx);
void trk_leaf(DBC *dbc, PAGE *h, db_indx_t p);
int crsr_nxt(DBC *dbc);
int crsr_prv(DBC *dbc);
int crsr_pf_nxt(DBC *dbc);
int crsr_pf_prv(DBC *dbc);
int crsr_jump(DBC *dbc);
#endif
