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

#ifndef INCLUDE_SC_TABLES_H
#define INCLUDE_SC_TABLES_H

int do_alter_table(struct ireq *iq, struct schema_change_type *s,
                   tran_type *tran);
int do_upgrade_table_int(struct schema_change_type *s);
int finalize_alter_table(struct ireq *iq, struct schema_change_type *s,
                         tran_type *tran);
int finalize_upgrade_table(struct schema_change_type *s);
#endif
