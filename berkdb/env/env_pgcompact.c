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

#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#endif

#include "db_int.h"
#include "dbinc/db_am.h"
#include "dbinc/log.h"
#include "dbinc/txn.h"
#include "dbinc_auto/dbreg_auto.h"
#include "dbinc_auto/dbreg_ext.h"
#include "dbinc_auto/db_ext.h"

#define NUM_MAX_RETRIES 5

/*
 * __dbenv_pgcompact --
 *  Compact page.
 *
 * PUBLIC: int __dbenv_pgcompact __P((DB_ENV *, int32_t, DBT *, double, double));
 */
int
__dbenv_pgcompact(dbenv, fileid, dbt, ff, tgtff)
	DB_ENV *dbenv;
    int32_t fileid;
	DBT *dbt;
	double ff;
	double tgtff;
{
	int ret, nretries;
	DB *dbp;
	DB_TXN *txn;

	nretries = 0;

retry:
	if (nretries++ > NUM_MAX_RETRIES)
		goto out;

	if ((ret = __txn_begin(dbenv, NULL, &txn, 0)) != 0) {
		__db_err(dbenv, "%s __txn_begin: %s", __func__, strerror(ret));
		return (ret);
	}

	/* We use the prefault routines below to solve the race where
	   the dbp is under page compaction while being removed from dbreg. */
	ret = __dbreg_id_to_db_prefault(dbenv, txn, &dbp, fileid, 0);

	if (ret != 0) {
		__db_err(dbenv, "%s __dbreg_id_to_db_prefault: %s %d",
				__func__, strerror(ret), fileid);
        goto err;
	}

	ret = __db_pgcompact(dbp, txn, dbt, ff, tgtff);

err:
	__dbreg_prefault_complete(dbenv, fileid);
	if (ret == 0)
		ret = __txn_commit(txn, DB_TXN_NOSYNC);
	else
		(void)__txn_abort(txn);
	txn = NULL;

	if (ret == DB_LOCK_DEADLOCK)
		/* If deadlock, retry. */
		goto retry;

out:
	return (ret);
}

/*
 * __dbenv_ispgcompactible --
 *	Return whether the given page is compactible.
 *
 * PUBLIC: int __dbenv_ispgcompactible __P((DB_ENV *, int32_t, db_pgno_t, DBT *, double));
 */
int
__dbenv_ispgcompactible(dbenv, fileid, pgno, dbt, ff)
	DB_ENV *dbenv;
	int32_t fileid;
	db_pgno_t pgno;
	DBT *dbt;
	double ff;
{
	int ret;
	DB *dbp;

	ret = __dbreg_id_to_db(dbenv, NULL, &dbp, fileid, 0, NULL, 0);
	if (ret != 0) {
		__db_err(dbenv, "%s __dbreg_id_to_db: %s %d",
				__func__, strerror(ret), fileid);
		return (ret);
	}

	ret = __db_ispgcompactible(dbp, pgno, dbt, ff);

	return ret;
}
