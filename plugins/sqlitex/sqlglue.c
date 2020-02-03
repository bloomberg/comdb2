#include <fcntl.h>
#include <sys/stat.h>
#include "sql.h"
#include "str0.h"

int sqlite4BtreeIsEmpty(BtCursor *pCur)
{
    return pCur->eof;
}

int sqlite3BtreeIsEOF(BtCursor *pCur)
{
   return pCur->eof;
}

int sqlite3BtreeIsEmpty(BtCursor *pCur)
{
   return pCur->empty;
}

int sqlite3BtreeRecordIDString(BtCursor *pCur,
      unsigned long long rowid,
      char **memp,
      size_t maxsz)
{
   unsigned long long prgenid; /* it's always printed & returned in big-endian */
   int rc;

   if (maxsz == 0) {
      maxsz = 64;
      *memp = sqlitexMalloc(maxsz);
   }

   pCur->vdbe = pthread_getspecific(sqlitexVDBEkey);

   /* assert that all my assumptions are true */
   assert(sizeof(pCur->rrn) <= sizeof(int));
   assert(sizeof(pCur->genid <= sizeof(unsigned long long)));

   if (!pCur->bt->is_temporary && pCur->cursor_class == CURSORCLASS_TABLE) {
      rc = enque_pfault_olddata_oldkeys(pCur->db, rowid, 0, -1, 0, 1, 1, 1);
   }
   prgenid = flibc_htonll(rowid);
   snprintf(*memp, maxsz, "2:%llu", prgenid);
   return SQLITE_OK;
}

/*
 ** This is the version of 'BeginTrans' which is invoked from
 ** OP_Ephemeral.  Previously sqlite would call the normal
 ** sqlite3BtreeBeginTrans, and always set the wrflag. We use this flag
 ** in snapshot/serializable mode to sniff out writes, and defer returning
 ** information about how many rows were written until after we've finished
 ** retrying. Opening an ephemeral table shouldn't affect this, so ignore
 ** the wrflag in this case.
 */
int sqlite3BtreeBeginTransNoflag(Vdbe *v, Btree *pBt)
{
   return sqlite3BtreeBeginTrans(v, pBt, 0, 0);
}

int is_remote(BtCursor *pCur)
{
   return pCur->cursor_class == CURSORCLASS_REMOTE;
}

int is_raw(BtCursor *pCur)
{
   if (pCur) {
      if (pCur->cursor_class == CURSORCLASS_TABLE) {
         return 1;
      } else if (pCur->cursor_class == CURSORCLASS_INDEX) {
         return 1;
      } else if (pCur->cursor_class == CURSORCLASS_REMOTE) {
         return 1;
      } else if (pCur->is_sampled_idx) {
         return 1;
      }
   }
   return 0;
}

/*
** Don't lock around access to sqlite_temp_master.
** Those are temp tables, but each thread makes its
** own copy and don't need to synchronize access to it.
*/
static __thread int tmptbl_use_lk = 0;
void comdb2_use_tmptbl_lk(int use)
{
   tmptbl_use_lk = use; //don't use lk for sqlite_temp_master
}

/* add the costs of the sorter to the thd costs */
void addVbdeToThdCost(int type)
{
    struct sql_thread *thd;
    thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return;

    double cost = 0;
    if(type == VDBESORTER_WRITE)
        thd->cost += 0.2;
    else if(type == VDBESORTER_MOVE || type == VDBESORTER_FIND)
        thd->cost += 0.1;
}

/**
 * Callback for sqlite during prepare, to retrieve default tzname
 * Required by stat4 which need datetime conversions during prepare
 *
 */
void comdb2_set_sqlite_vdbe_tzname(Vdbe *p)
{
   struct sql_thread *sqlthd = pthread_getspecific(query_info_key);

   if(!sqlthd)
      return;

   /*prepare the timezone info*/
   memcpy(p->tzname, sqlthd->clnt->tzname, TZNAME_MAX);
}

/* append the costs of the sorter to the thd query stats */
void addVbdeSorterCost(const VdbeSorter *pSorter)
{
    struct sql_thread *thd;
    thd = pthread_getspecific(query_info_key);
    if (thd == NULL)
        return;

    struct query_path_component fnd={{0}}, *qc;

    if(NULL == (qc = hash_find(thd->query_hash, &fnd)))
    {
       qc = calloc(sizeof(struct query_path_component), 1);
       hash_add(thd->query_hash, qc);
       listc_abl(&thd->query_stats, qc);
    }

    qc->nfind += pSorter->nfind;
    qc->nnext += pSorter->nmove;
    /* note: we record writes in record routines on the master */
    qc->nwrite += pSorter->nwrite;
}

extern int gbl_abort_on_dta_lookup_error;
void abort_on_dta_error() {
    char cbuf[64];
    struct stat statbuf;
    int rc;
    if (gbl_abort_on_dta_lookup_error) {
        snprintf0(cbuf,sizeof(cbuf),"%s.dtaerror",thedb->envname);
        rc = stat(cbuf, &statbuf);
        if (rc && errno == ENOENT) {
            creat(cbuf,0666);
            abort();
        }
    }
}
