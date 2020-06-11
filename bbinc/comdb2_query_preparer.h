/*
   Copyright 2019, 2020 Bloomberg Finance L.P.

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

#ifndef __INCLUDED_QUERY_PREPARER_H
#define __INCLUDED_QUERY_PREPARER_H

struct comdb2_query_preparer {
    int (*do_prepare)(struct sqlthdstate *, struct sqlclntstate *, const char *,
                      char ***, int *);
    int (*do_cleanup)(struct sqlclntstate *);
    int (*sqlitex_is_initializing)(void *);
    char *(*sqlitex_table_name)(void *);
};
typedef struct comdb2_query_preparer comdb2_query_preparer_t;

extern comdb2_query_preparer_t *query_preparer_plugin;
extern int gbl_old_column_names;
#endif /* !__INCLUDED_QUERY_PREPARER_H */
