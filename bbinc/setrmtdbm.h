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

/* pl/setrmtdbm.h - Functions to set the PRCCOM header. */

#ifndef INCLUDED_SETRMTDBM_H
#define INCLUDED_SETRMTDBM_H

#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Set the length of the buffer in the PRCCOM header - len should be between
 * 0 and 64KB.
 */
void setrmtlen_(int *len, int *dbptr);
void setrmtlen(int len, int dbptr);
void setrmtlen64(int len, intptr_t dbptr);
void setrmtlen64_(int32_t *len, intptr_t *dbptr);
void setrmtlenc_(int *len, void *bufptr);
void setrmtlenc(int len, void *bufptr);
void setrmtlenusr(int len, int dbptr);
void setrmtlenusrc(int len, void *buf);

/*
 * Set the remote database destination to *db.
 */
void setrmtdb_(int *db, int *dbptr);
void setrmtdb(int db, int dbptr);
void setrmtdb64(int db, intptr_t dbptr);

/*
 * Set the destination machine node number to *rmtmach.
 */
void setrmtm_(int *rmtmach, int *dbptr);
void setrmtm(int machine, int dbptr);
void setrmtmc_(int *machine, void *buf);
void setrmtmc(int machine, void *buf);

/*
 * Set the destination machine, db and buffer length.
 */
void setrmtdbm_(int *db, int *mch, int *len, int *dbptr);
void setrmtdbm(int db, int machnum, int len, int dbptr);
void setrmtdbm64(int db, int machnum, int len, intptr_t dbptr);
void setrmtdbmc_(int *db, int *machnum, int *len, void *buf);
void setrmtdbmc(int db, int machnum, int len, void *buf);
void setrmtdbmusr(int db, int machnum, int len, int dbptr);
void setrmtdbmusrc(int db, int cpu, int len, void *buf);

#ifdef __cplusplus
}
#endif

#endif
