#include "db_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/lock.h"
#include <btree/bt_prefix.h>

#include <stdio.h>
#include <stdlib.h>

#include "dbinc/btree.h"
#include "dbinc/mp.h"
#include <btree/bt_pf.h>

#include "dbinc/btree.h"
#include "logmsg.h"

extern struct thdpool *gbl_udppfault_thdpool;

static inline int chk_forward(DBC *dbc);
static inline int chk_backward(DBC *dbc);
static inline int adj_wndw(DBC *dbc, btpf * f);
static inline void btpf_copy(btpf * pf, btpf * npf);
static inline int page_load_f(btpf * pf, DBC *dbc);
static inline int page_load_b(btpf * pf, DBC *dbc);
static inline int tree_walk(DBC *dbc, btfp_tw_flag fl, db_pgno_t root_p,
    u_int8_t lev);
btpf_job *btpf_job_init(void);
void btpf_free_job(btpf_job ** x);
static inline int start_loading(DBC *dbc);
void start_loading_async_pp(struct thdpool *pool, void *work, void *thddata,
    int op);
void start_loading_async_cb(btpf_job * job);
static inline int advance_on_tree(DBC *dbc);

#define LOAD(mpf,x) enqueue_touch_page(mpf, x);

#define LOAD_SYNC(mpf,x,page) {                                                     \
    __memp_fget(mpf, &x, DB_MPOOL_PFGET, &page);                                    \
    __memp_fput(mpf,page, 0);                                                       \
}                                                                                   \

#define BTPF_DEBUG 0
#define BTPF_SAME_THREAD 0

#define TEST_STOP(dbc) {                                                             \
    if (!BTPF_ENABLED(dbc) || PFX(dbc)->curlf[MIN_TH(dbc)] == PGNO_INVALID)           \
    {                                                                                \
        PFX(dbc)->on = PF_OFF;                                                        \
        PFX(dbc)->status = STOPPED;                                                   \
    }                                                                                \
}

#define SANITY_CHECK(i,rst,pf) {                                                    \
    for(i=2; i < RMBR_LVL; i++)                                                     \
    {                                                                               \
        if (pf->curlf[i-1] == 0 && pf->curlf[i] != 0)                               \
            rst = 1;                                                                \
    }                                                                               \
    btpf_fprintf(stderr,pf);                                                        \
}                                                                                   \


btpf *
btpf_init(void)
{
	btpf *x = (btpf *) malloc(sizeof(btpf));

	if (x) {
		memset(x, 0, sizeof(btpf));
		x->on = PF_ON;
	}
	return x;
}

void
btpf_free(btpf ** x)
{
	free(*x);
	*x = NULL;
}



btpf_job *
btpf_job_init(void)
{
	btpf *pf = btpf_init();

	if (pf == NULL)
		return NULL;
	btpf_job *x = (btpf_job *) malloc(sizeof(btpf_job));

	if (x) {
		memset(x, 0, sizeof(btpf_job));
		x->pf = pf;
	}
	return x;
}

void
btpf_free_job(btpf_job ** x)
{
	if (*x == NULL)
		return;
	if ((*x)->rkey != NULL) {
		if ((*x)->rkey->data != NULL)
			free((*x)->rkey->data);
		free((*x)->rkey);
	}
	free(*x);
	*x = NULL;
}

void
btpf_copy_dbc(DBC *dbc, DBC *ndbc)
{
	btpf_copy(PFX(dbc), PFX(ndbc));
}

void
btpf_copy(btpf * pf, btpf * npf)
{
	if (!pf || !npf)
		return;
	memcpy(npf, pf, sizeof(btpf));
}

void
btpf_fprintf(FILE *f, btpf * x)
{
	int i;
	fprintf(f, "pf:%d wndw:%d rdr_rec_cnt:%d rdr_pg_cnt:%d\n", x->status,
		x->wndw, x->rdr_rec_cnt,x->rdr_pg_cnt );
	for (i=0; i < RMBR_LVL; i++)
		fprintf(f, "%d %d %d %d\n", i, x->curlf[i],
			x->curindx[i], x->maxindx[i]);
}

void
btpf_rst(btpf * x)
{
	x->status = INIT;
	x->direction = UNSET;
	x->rdr_rec_cnt = 0;
	x->rdr_pg_cnt = 0;
	x->wndw = 0;
	x->tr_page = PGNO_INVALID;
	x->on = PF_ON;
	// TODO update stats
}

static inline void
btpf_cnt_rst(btpf * pf)
{
	pf->rdr_rec_cnt = 0;
	pf->rdr_pg_cnt = 0;
}

int
crsr_nxt(DBC *dbc)
{
	int ret = 0;
	if (!PFX(dbc))
		return 0;
	TEST_STOP(dbc);
	if (!PFX(dbc)->on)
		return 100;
	if (PFX(dbc)->status == LOADED_ALL)
		return 0;
	if (PFX(dbc)->direction == BACKWARD) {
		btpf_rst(PFX(dbc));
		if (PFX(dbc)->status == PF || PFX(dbc)->status == LOADED_ALL) 
			ret = tree_walk(dbc, SRCH_CUR, 1, RMBR_LVL );
	}

	PFX(dbc)->direction = FORWARD; 
	if (!ret) {
		PFX(dbc)->rdr_rec_cnt++;
		chk_forward(dbc);
	}

	return ret;
}

int
crsr_prv(DBC *dbc)
{
	int ret = 0;
	if (!PFX(dbc))
		return 0;
	TEST_STOP(dbc);
	if (!PFX(dbc)->on)
		return 100;
	if (PFX(dbc)->status == LOADED_ALL)
		return 0;
	if (PFX(dbc)->direction == FORWARD) {
		btpf_rst(PFX(dbc));
		if (PFX(dbc)->status == PF || PFX(dbc)->status == LOADED_ALL)
			ret = tree_walk(dbc, SRCH_CUR, 1, RMBR_LVL );
	}

	PFX(dbc)->direction = BACKWARD;

	if (!ret) {
		PFX(dbc)->rdr_rec_cnt++;
		ret = chk_backward(dbc);
	}
   
	return ret;
}

int
crsr_pf_nxt(DBC *dbc)
{
	int ret = 0;
	if (!PFX(dbc))
		return 0;
	TEST_STOP(dbc);
	if (!PFX(dbc)->on)
		return 100;
	if (PFX(dbc)->status == LOADED_ALL)
		return 0;
	if (PFX(dbc)->direction == BACKWARD) {
		btpf_rst(PFX(dbc));
		if (PFX(dbc)->status == PF || PFX(dbc)->status == LOADED_ALL)
			ret = tree_walk(dbc, SRCH_CUR, 1, RMBR_LVL );
	}
	PFX(dbc)->direction = FORWARD;  
	if (!ret) {
		PFX(dbc)->rdr_pg_cnt++;
		PFX(dbc)->rdr_rec_cnt++;
		ret = chk_forward(dbc);
	}
#if BTPF_DEBUG 
	fprintf(stderr, "Moving to next page ");
	btpf_fprintf(stderr, PFX(dbc));
#endif   
	return ret;
}

int
crsr_pf_prv(DBC *dbc)
{
	int ret = 0;
	if (!PFX(dbc))
		return 0; 
	TEST_STOP(dbc);
	if (!PFX(dbc)->on)
		return 100;
	if (PFX(dbc)->status == LOADED_ALL)
		return 0;
	if (PFX(dbc)->direction == FORWARD) {

		btpf_rst(PFX(dbc));
		if (PFX(dbc)->status == PF || PFX(dbc)->status == LOADED_ALL)
			ret = tree_walk(dbc, SRCH_CUR, 1, RMBR_LVL );
	} 

	PFX(dbc)->direction = BACKWARD;

	if (!ret) {
		PFX(dbc)->rdr_pg_cnt++;
		PFX(dbc)->rdr_rec_cnt++;
		ret = chk_backward(dbc);
	}
#if BTPF_DEBUG 
	fprintf(stderr, "Moving to previous page");
	btpf_fprintf(stderr, PFX(dbc));
#endif   
	return ret;
}

int
crsr_jump(DBC *dbc)
{
	if (!PFX(dbc))
		return 0;
	TEST_STOP(dbc)
	    btpf_rst(PFX(dbc));

	return (0);
}

static inline int
chk_forward(DBC *dbc)
{
	int fetch = 0;
	btpf *f = PFX(dbc);
	BTREE_CURSOR *cp = (BTREE_CURSOR *)dbc->internal;
	int rst = 0;
	int32_t th;

	if (f->status == LOADED_ALL)
		return rst;

	th = f->wndw - f->rdr_pg_cnt - CU_GAP(dbc);  
	fetch = (th <= 0);

#if BTPF_DEBUG
	fprintf(stderr, "wndw: %d, rdr_pg_cnt: %u, dist_pf:%d", f->wndw, f->rdr_pg_cnt, th );
#endif

	if (f->status == INIT)
		fetch |= ((NUM_ENT(cp->page) - cp->indx) / P_INDX)
			< PG_GAP(dbc);
	if (fetch) {  
		adj_wndw(dbc, f);
		start_loading(dbc);
		f->status = PF;
		btpf_cnt_rst(f);
	}
	return rst;
}

static inline int
chk_backward(DBC *dbc)
{
	int fetch = 0;
	btpf *f = PFX(dbc);
	BTREE_CURSOR *cp = (BTREE_CURSOR *)dbc->internal;
	int rst = 0;
	int32_t th;

	if (f->status == LOADED_ALL)
		return rst;    
        
	th = f->wndw - f->rdr_pg_cnt - CU_GAP(dbc);
	fetch = (th <= 0);

#if BTPF_DEBUG
	fprintf(stderr, "wndw: %u, rdr_pg_cnt: %u, dist_pf:%d \n", f->wndw, f->rdr_pg_cnt, th );
#endif

	if (f->status == INIT)
		fetch |= (cp->indx / P_INDX) < PG_GAP(dbc);
	if (fetch)
	{   
		adj_wndw(dbc,f);
		start_loading(dbc);
		f->status = PF;
		btpf_cnt_rst(f);
	}
	return rst;
}

static inline int
adj_wndw(DBC *dbc, btpf * f)
{
	if (f->wndw == 0) {
		f->wndw = WNDW_MIN(dbc);
	} else {
		f->wndw *=  WNDW_INC(dbc);
		f->wndw = f->wndw > WNDW_MAX(dbc) ? WNDW_MAX(dbc) : f->wndw;
	}
#if BTPF_DEBUG 
	fprintf(stderr, "Adapting window to %d\n", f->wndw);
#endif
	return (0);
}

void
trk_descent(DB *dbp, DBC *dbc, PAGE *h, db_indx_t indx)
{
	int level = h->level -1;
	if (level < RMBR_LVL) {
		PFX(dbc)->curlf[level] = h->pgno;
		PFX(dbc)->curindx[level] = indx;
		PFX(dbc)->maxindx[level] = h->entries;
		memcpy(&(PFX(dbc)->lsn[level]), &(h->lsn), sizeof(DB_LSN));
	}
#if BTPF_DEBUG 
	btpf_fprintf(stderr, PFX(dbc));
#endif 
}

void
trk_leaf(DBC *dbc, PAGE *h, db_indx_t p)
{
	PFX(dbc)->curlf[0] = h->pgno;
	PFX(dbc)->curindx[0] = p;
	PFX(dbc)->maxindx[0] = NUM_ENT(h);
	memcpy(&(PFX(dbc)->lsn[0]), &(h->lsn), sizeof(DB_LSN));
}

static inline int
read_key(DBC *dbc, btpf_job * job)
{
	BKEYDATA *bk;
	u_int8_t buf[KEYBUF];
	PAGE *h = dbc->internal->page;
	DB *dbp = dbc->dbp;
	job->rkey = (DBT*) malloc(sizeof(DBT));

#if BTPF_DEBUG     
	fprintf(stderr, "Starting from page: %d \n", h->pgno);
#endif        
	if (job->rkey == NULL)
		return 1;
	memset(job->rkey,0,sizeof(DBT));

	bk = GET_BKEYDATA(dbp, h, 0);

	bk_decompress(dbp, h, &bk, buf, KEYBUF);
 
	ASSIGN_ALIGN_DIFF(u_int32_t, job->rkey->size, db_indx_t, bk->len);
	job->rkey->data = malloc(job->rkey->size);

	memcpy(job->rkey->data, bk->data, job->rkey->size);
	return 0;
}

static inline int
start_loading(DBC *dbc)
{
	int rc = 0;
	btpf_job *job = btpf_job_init();

	btpf_copy(PFX(dbc), PFX(job));
	read_key(dbc, job);
	job->npages = PFX(dbc)->wndw;
	job->mpf = dbc->dbp->mpf;
	job->db = dbc->dbp;
	job->dirty = PFX(dbc)->status == PF || PFX(dbc)->status == LOADED_ALL;	// TODO it cannot be on LOADED_ALL when it runs asynchronously
	job->lid = dbc->lid;
	job->locker = dbc->locker;
#if BTPF_SAME_THREAD
	start_loading_async_cb(job);
#else
	rc = thdpool_enqueue(gbl_udppfault_thdpool, start_loading_async_pp, job,
	    0, NULL, 0, PRIORITY_T_DEFAULT);
#endif
	return rc;
}

void
start_loading_async_pp(struct thdpool *pool, void *work, void *thddata, int op)
{

	btpf_job *job = ((btpf_job *) work);

	switch (op) {
	case THD_RUN:
		start_loading_async_cb(job);
		break;
	}
    btpf_free_job(&job);

}

void
start_loading_async_cb(btpf_job * job)
{
	int rc = 0;
	DBC *dbc;
	DB *db = job->db;

#if BTPF_SAME_THREAD
	if ((rc =
		job->db->paired_cursor_from_lid(job->db, job->locker, &dbc,
		    DB_PAUSIBLE)) != 0)
		return;
#else
	if ((rc = db->cursor(db, NULL, &dbc, 0)) != 0)
		 return;
#endif

	dbc->rkey = job->rkey;
	if (job->dirty) {
		if ((rc = tree_walk(dbc, SRCH_CUR, 1, RMBR_LVL)) != 0)
			return;
	} else {
		btpf_copy(PFX(job), PFX(dbc));
	}

	PFX(dbc)->direction = job->pf->direction;
	PFX(dbc)->wndw = job->npages;

#if BTPF_DEBUG 
	fprintf(stderr, "Found rec in page: %d \n", PFX(dbc)->curlf[0]);
#endif

	if (PFX(dbc)->direction == FORWARD)
		page_load_f(PFX(dbc), dbc);
	else
		page_load_b(PFX(dbc), dbc);

}

static inline int
advance_on_tree(DBC *dbc)
{
	DB *dbp = dbc->dbp;
	DB_MPOOLFILE *mpf = dbp->mpf;
	btpf *pf = PFX(dbc);
	DB_LOCK lock;
	PAGE *h;
	db_pgno_t pgno;
	db_lockmode_t lock_mode = DB_LOCK_READ;
	u_int8_t up_level = 1;
	int ret = 0;
	
	while (up_level < RMBR_LVL && 
	       (pf->curindx[up_level] >= pf->maxindx[up_level] -1 ) && 
	       pf->curlf[up_level] > 1 ) 
		up_level++;
   
	if (up_level >= RMBR_LVL || 
	    pf->curindx[up_level] >= pf->maxindx[up_level] -1) {
#if BTPF_DEBUG 
		fprintf(stderr, "Reached end of the tree, level: %d  index: %d "
			"maxindex:%d moving page:%d FORWARD\n ", up_level, 
			pf->curindx[up_level], pf->maxindx[up_level], 
			pf->curlf[up_level]);
		btpf_fprintf(stderr,pf);
#endif 
		pf->status = LOADED_ALL;
		ret = END_OF_TREE;
		goto end;
	}
            
	pf->curindx[up_level] += 1;
    
	if (up_level > 1)
	{
		pgno = pf->curlf[up_level];
		if ((ret = __db_lget(dbc, 0, pgno, lock_mode, 0, &lock)) != 0)
			goto end;

		if ((ret = __memp_fget(mpf, &pgno, 0, &h)) == 0)
		{   
			if (memcmp(&h->lsn, &pf->lsn[up_level], sizeof(DB_LSN)) == 0)
			{
				pgno = GET_BINTERNAL(dbp, h, pf->curindx[up_level])->pgno; 
				pf->curlf[up_level -1] = pgno;

				(void)__memp_fput(mpf, h, 0);
				ret = tree_walk(dbc, FIRST, pf->curlf[up_level - 1], up_level );
			} else 
			{
				(void)__memp_fput(mpf, h, 0);
				ret = DIFF_LSN;
			}
            
                      
		}
		(void)__LPUT(dbc, lock);  
	}
end:
	if (ret > 0 && ret != END_OF_TREE && ret != DIFF_LSN)
		logmsg(LOGMSG_ERROR, "%s return code: %d \n", __func__, ret);

	return ret;
}

static inline int
advanceb_on_tree(DBC *dbc)
{
	DB *dbp = dbc->dbp;
	DB_MPOOLFILE *mpf = dbp->mpf;
	btpf *pf = PFX(dbc);
	DB_LOCK lock;
	PAGE *h;
	db_pgno_t pgno;
	db_lockmode_t lock_mode = DB_LOCK_READ;

	u_int8_t up_level = 1;
	int ret = 0;

	up_level = 1;

	while(up_level < RMBR_LVL && pf->curindx[up_level] == 0 && pf->curlf[up_level] > 1)
		up_level++;
    
	if (up_level >= RMBR_LVL || pf->curindx[up_level] == 0)
	{
		pf->status = LOADED_ALL;
#if BTPF_DEBUG 
		fprintf(stderr, "Reached end of the tree, level: %d moving: BACKWARD\n ", up_level);
#endif 
		ret = END_OF_TREE;
		goto end;
	}
            
	pf->curindx[up_level] -= 1;
    
	if (up_level > 1)
	{
		pgno = pf->curlf[up_level];
		if ((ret = __db_lget(dbc, 0, pgno, lock_mode, 0, &lock)) != 0)
			goto end;
        
		if ((ret = __memp_fget(mpf, &pgno, 0, &h)) == 0)
		{    
            
			if (memcmp(&h->lsn, &pf->lsn[up_level], sizeof(DB_LSN)) == 0)
			{
				pgno = GET_BINTERNAL(dbp, h, pf->curindx[up_level])->pgno; 
				pf->curlf[up_level -1] = pgno; 
              
				(void)__memp_fput(mpf, h, 0);
				ret = tree_walk(dbc, LAST, pf->curlf[up_level - 1], up_level); 
			} else
			{
				(void)__memp_fput(mpf, h, 0);
				ret = DIFF_LSN;
			}
		} 
		(void)__LPUT(dbc, lock);  
	}
end:
	if (ret > 0 && ret != END_OF_TREE && ret != DIFF_LSN)
		logmsg(LOGMSG_ERROR, "%s return code: %d \n", __func__, ret);

	return ret;
}


static inline int
page_load_f(btpf * pf, DBC *dbc)
{
	DB *dbp = dbc->dbp;
	DB_MPOOLFILE *mpf = dbp->mpf;

	DB_LOCK lock;
	PAGE *h;
	db_pgno_t pgno;
	db_pgno_t t_pgno;
	db_lockmode_t lock_mode = DB_LOCK_READ;
	int ret = 0;
	db_indx_t p_cnt = 0;
	db_indx_t c = 0;
	db_indx_t i;

	while (1) {
		if ((ret = advance_on_tree(dbc)) != 0)
			goto end;
            
		pgno = pf->curlf[1];
		if ((ret = __db_lget(dbc, 0, pgno, lock_mode, 0, &lock)) != 0)
			goto end;

        
		if ((ret = __memp_fget(mpf, &pgno, 0, &h)) != 0 || memcmp(&h->lsn, &pf->lsn[1], sizeof(DB_LSN)) != 0)
		{
			(void)__memp_fput(mpf, h, 0);
			(void)__LPUT(dbc, lock);
			ret = DIFF_LSN;
			goto end;
		}
        
		p_cnt = pf->maxindx[1] - pf->curindx[1];
		p_cnt = p_cnt > pf->wndw - c ? pf->wndw - c : p_cnt;

		for (i = 0; i < p_cnt; i++)
		{
			t_pgno = GET_BINTERNAL(dbp, h, pf->curindx[1] + i)->pgno;
#if BTPF_DEBUG  
			fprintf(stderr, "LOADING: %u from:%u indx:%d of:%d real:%d\n", t_pgno, pgno, pf->curindx[1] + i, pf->maxindx[1], h->entries );
#endif
			LOAD(mpf, t_pgno);

		}

		c += p_cnt;
		pf->curindx[1] += p_cnt;
		(void)__memp_fput(mpf, h, 0);
		(void)__LPUT(dbc, lock);

		if (c >= pf->wndw)
			break;
	}
end:
	if (ret > 0 && ret != END_OF_TREE && ret != DIFF_LSN)
		logmsg(LOGMSG_ERROR, "%s return code: %d \n", __func__, ret);
#if BTPF_DEBUG
	if (ret == DIFF_LSN)
		fprintf(stderr,
		    "%s shutting PF OFF because LSN differs from original \n",
		    __func__);
	if (ret == END_OF_TREE)
		fprintf(stderr,
		    "%s shutting PF OFF because cursor reached the end of the treei \n",
		    __func__);
#endif

	if (ret != 0) {
		pf->on = PF_OFF;
	}
    
	ret = __db_c_close(dbc);
    
	if (ret)
		logmsg(LOGMSG_ERROR, "%s bt_pf cannot close cursor\n", __func__);

	return ret;
}

static inline int
page_load_b(btpf * pf, DBC *dbc)
{
	DB *dbp = dbc->dbp;
	DB_MPOOLFILE *mpf = dbp->mpf;

	DB_LOCK lock;
	PAGE *h;
	db_pgno_t pgno;
	db_pgno_t t_pgno = 0;
	db_lockmode_t lock_mode = DB_LOCK_READ;


	int ret = 0;
	db_indx_t p_cnt = 0;
	db_indx_t c = 0;
	db_indx_t i;

	while (1) {
		if ((ret = advanceb_on_tree(dbc)) != 0)
			goto end;

		pgno = pf->curlf[1];
		if ((ret = __db_lget(dbc, 0, pgno, lock_mode, 0, &lock)) != 0) {
			(void)__LPUT(dbc, lock);
			goto end;
		}
		if ((ret = __memp_fget(mpf, &pgno, 0, &h)) != 0 ||
		    memcmp(&h->lsn, &pf->lsn[1], sizeof(DB_LSN)) != 0) {
			(void)__memp_fput(mpf, h, 0);
			(void)__LPUT(dbc, lock);
			ret = DIFF_LSN;
			goto end;
		}

		p_cnt = pf->curindx[1] > pf->wndw - c ? pf->curindx[1] - pf->wndw - c : 0;
		for (i = pf->curindx[1] ; i >= p_cnt ; i--) {
			if (pf->maxindx[1] == 0)
				break;
			t_pgno = GET_BINTERNAL(dbp, h, i)->pgno;
			if (i >= pf->maxindx[1])
				continue;
#if BTPF_DEBUG  
			fprintf(stderr, "LOADING: %u from:%u indx:%d of:%d real:%d\n", t_pgno, pgno, i, pf->maxindx[1], h->entries );
#endif            
			LOAD(mpf,t_pgno);

			if (i == 0)
				break; // it's an unsigned type it overflows and loop forever otherwise
		}
		pf->tr_page = t_pgno;
		c += (pf->curindx[1] - p_cnt);
		pf->curindx[1] = p_cnt;

		(void)__memp_fput(mpf, h, 0);
		(void)__LPUT(dbc, lock);  // release lock

		if (c >= pf->wndw)
			break;
	}
end:
	if (ret > 0 && ret != END_OF_TREE && ret != DIFF_LSN)
		logmsg(LOGMSG_ERROR, "%s return code: %d \n", __func__, ret);
#if BTPF_DEBUG
	if (ret == DIFF_LSN)
		fprintf(stderr,
		    "%s shutting PF OFF because LSN differs from original \n",
		    __func__);
	if (ret == END_OF_TREE)
		fprintf(stderr,
		    "%s shutting PF OFF because cursor reached the end of the tree \n",
		    __func__);
#endif
	if (ret != 0) {
		pf->on = PF_OFF;
	}
	ret = __db_c_close(dbc);

	if (ret)
		logmsg(LOGMSG_ERROR, "%s bt_pf cannot close cursor\n", __func__);

	return ret;
}


/*
 * Tree_walk 
 * Record tree descent  
 */
static inline int
tree_walk(DBC *dbc, btfp_tw_flag fl, db_pgno_t root_p, u_int8_t lev)
{
	DBC *t_dbc = NULL;
	int ret;

	if ((ret =
		dbc->dbp->paired_cursor_from_lid(dbc->dbp, dbc->locker, &t_dbc,
		    0)) != 0)
		return (ret);

	t_dbc->internal->root = root_p;
	t_dbc->internal->pgno = root_p;
	t_dbc->rkey = dbc->rkey;
	PFX(t_dbc)->on = PF_OFF;

	if (fl == SRCH_CUR) {
		ret = t_dbc->c_am_get(t_dbc, t_dbc->rkey, NULL, DB_SET, 0);
	} else if (fl == FIRST) {
		ret = t_dbc->c_am_get(t_dbc, NULL, NULL, DB_FIRST, 0);

	} else if (fl == LAST) {
		ret = t_dbc->c_am_get(t_dbc, NULL, NULL, DB_LAST, 0);
	}
	if (!ret) {
		int x;

		for (x = 0; x < lev; x++) {
			PFX(dbc)->curlf[x] = PFX(t_dbc)->curlf[x];
			PFX(dbc)->curindx[x] = PFX(t_dbc)->curindx[x];
			PFX(dbc)->maxindx[x] = PFX(t_dbc)->maxindx[x];
			memcpy(&(PFX(dbc)->lsn[x]), &(PFX(t_dbc)->lsn[x]),
			    sizeof(DB_LSN));
		}
		PFX(dbc)->tr_page = t_dbc->internal->pgno;
	}

	if (ret != 0)
		logmsg(LOGMSG_ERROR, "You got a problem young man! ");

	if (ret != 0)
		logmsg(LOGMSG_ERROR, "%s return code: %d \n", __func__, ret);

	__db_c_close(t_dbc);

	return ret;
}
