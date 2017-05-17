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

#ifndef __COMDB2_SQL_INTERNAL_H
#define __COMDB2_SQL_INTERNAL_H

#include <sqlite3.h>

#include <list.h>

typedef struct reqhdr {
    int reqtype;
    int len;
} reqhdr_type;

typedef struct rsphdr {
    int rcode;
    int len;
} rsphdr;

typedef struct finalize_request {
    int tranid;
} finalize_request_type;

typedef struct column_request {
    int tranid;
    int column;
} column_request_type;

/* request types */
enum {
    RSQLITE3_CLOSE = 1,    /* close connection */
    RSQLITE3_FINALIZE = 2, /* done with statement */
    RSQLITE3_PREPARE = 3,  /* start new statement */
    RSQLITE3_DECLTYPE = 4, /* return column type string */
    RSQLITE3_TEXT = 5,     /* get string value of column */
    RSQLITE3_ERRMSG = 6,   /* return error message string for last error */
    RSQLITE3_STEP = 7,     /* insert/fetch/update next record */
    RSQLITE3_EXEC = 8,     /* execute statment */
    RSQLITE3_CHANGES = 9,  /* #changed rows for exec */
};

/* field flags */
enum { SQLFIELD_NULL = 1 };

struct sqlite3_stmt {
    int tranid;
    struct sql_schema *schema;
    sqlite3 *db;
    char *debug_sql;
    LINKC_T(struct sqlite3_stmt) lnk;
};

struct sqlite {
    int fd;
    int ncols;
    char **colnames;
    char **colvalues;

    char *colnamebuf;
    int colnamebufsize;
    char *colvalbuf;
    int colvalbufsize;
    LISTC_T(struct sqlite3_stmt) cursors;
};

#endif
