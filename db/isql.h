#if !defined(ISQL_H_INCLUDED)
#define ISQL_H_INCLUDED
/*
   Copyright 2018 Bloomberg Finance L.P.

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

typedef struct Mem Mem;

enum {
    ISQL_EXEC_NORMAL, /* run_internal_sql() */
    ISQL_EXEC_QUICK,  /* run_verify_indexes_query() */
    ISQL_EXEC_INLINE,
};

typedef struct {
    struct schema *s;
    int col_count;
    int row_count;
    Mem *in;
    Mem *out;
} isql_data_t;

int isql_init(struct sqlclntstate *clnt);
int isql_destroy(struct sqlclntstate *clnt);
int isql_exec(struct sqlthdstate *thd, struct sqlclntstate *clnt);
int isql_run(struct sqlclntstate *clnt, const char *sql, int mode);

#endif /* !ISQL_H_INCLUDED */
