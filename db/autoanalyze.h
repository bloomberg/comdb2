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

#ifndef INCLUDE_AUTOANALYZE_H
#define INCLUDE_AUTOANALYZE_H

#include <comdb2.h>

int load_auto_analyze_counters(void);
void stat_auto_analyze(void);
void *auto_analyze_main(void *);
void *auto_analyze_table(void *);
void autoanalyze_after_fastinit(char *);
void get_auto_analyze_tbl_stats(struct dbtable *, int, int *, int *, unsigned int *, double *);

#endif // INCLUDE_AUTOANALYZE_H
