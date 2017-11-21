/*
  Copyright 2017, Bloomberg Finance L.P.

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

#ifndef _LOCKER_INFO_H
#define _LOCKER_INFO_H

#define	ISSET_MAP(M, N)	((M)[(N) / 32] & (1 << (N) % 32))

#include "build/db.h"
#ifndef _DB_LOCK_H_
typedef struct DB_LOCKOBJ DB_LOCKOBJ;
#endif

typedef struct {
	DB_LOCKOBJ *last_obj;
	pthread_t tid;
	snap_uid_t *snap_info; /* contains cnonce */
	roff_t last_lock;
	u_int32_t count;
	u_int32_t id;
	u_int32_t last_locker_id;
	db_pgno_t pgno;
	int killme;
	int saveme;
	int readonly;
	u_int8_t self_wait;
	u_int8_t valid;
	u_int8_t in_abort;
	u_int8_t tracked;
} locker_info;

#endif
