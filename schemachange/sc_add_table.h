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

#ifndef INCLUDE_SC_ADD_TABLES_H
#define INCLUDE_SC_ADD_TABLES_H

int do_add_table(struct ireq *, struct schema_change_type *, tran_type *);
int add_table_to_environment(char *table, const char *csc2,
                             struct schema_change_type *s, struct ireq *iq,
                             tran_type *trans);
int finalize_add_table(struct ireq *, struct schema_change_type *, tran_type *);

#endif
