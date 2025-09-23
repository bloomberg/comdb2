/*
   Copyright 2023 Bloomberg Finance L.P.

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

#ifndef PHYS_REP_H
#define PHYS_REP_H

#include <stdlib.h>
#include <cdb2api.h>

extern char *gbl_physrep_source_dbname;
extern char *gbl_physrep_source_host;
extern char *gbl_physrep_metadb_name;
extern char *gbl_physrep_metadb_host;

extern int gbl_is_physical_replicant;
extern int gbl_physrep_debug;
extern int gbl_deferred_phys_flag;

extern unsigned int gbl_deferred_phys_update;

int start_physrep_threads();
int stop_physrep_threads();
int physrep_exited();
int physrep_get_metadb_or_local_hndl(cdb2_hndl_tp**);
void physrep_cleanup(void);
void physrep_update_low_file_num(int*, int*);
void physrep_fanout_override(const char *dbname, int fanout);
int physrep_fanout_get(const char *dbname);
void physrep_fanout_dump(void);
int physrep_add_alternate_metadb(char *dbname, char *host);

#endif /* PHYS_REP_H */
