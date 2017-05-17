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

#ifndef _SQLSTAT1_H_
#define _SQLSTAT1_H_

#include "comdb2.h"

/**
 * Retrives this sqlstat record from
 *
 */
int sqlstat_find_record(struct ireq *iq, void *trans, const void *rec,
                        unsigned long long *genid);

int sqlstat_save_previous(struct ireq *iq, void *trans,
                          unsigned long long genid);

int sqlstat_delete_previous(struct ireq *iq, void *trans, const void *rec);
void *get_field_from_sqlite_stat_rec(struct ireq *iq, const void *rec,
                                     const char *fld);

int stat1_ondisk_record(struct ireq *iq, char *tbl, char *ix, char *stat,
                        void **out);
int sqlstat_find_get_record(struct ireq *iq, void *trans, void *rec,
                            unsigned long long *genid);

#endif
