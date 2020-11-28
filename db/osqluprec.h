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

#ifndef _OSQL_UPREC_H_
#define _OSQL_UPREC_H_

#include "comdb2.h"

/* Offload record upgrade statistics */
void upgrade_records_stats(void);

/* Offload upgrade record request. */
int offload_comm_send_upgrade_records(const dbtable *db,
                                      unsigned long long genid);

/* Offload upgrade record request. */
int offload_comm_send_upgrade_record(const char *tbl, unsigned long long genid);

#endif
