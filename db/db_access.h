/*
   Copyright 2021 Bloomberg Finance L.P.

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

#ifndef __INCLUDED_DB_AUTH_H
#define __INCLUDED_DB_AUTH_H

extern int gbl_check_access_controls;

/* Authentication types for users */
enum { AUTH_READ = 1, AUTH_WRITE = 2, AUTH_OP = 3, AUTH_USERSCHEMA = 4 };

enum PRIVILEGE_TYPE {
    PRIV_INVALID,
    PRIV_READ = 1 << 0,
    PRIV_WRITE = 1 << 1,
    PRIV_DDL = 1 << 2,
};

typedef struct table_permission {
    char table_name[MAXTABLELEN];
    uint8_t privileges;
} table_permissions_t;

void check_access_controls(struct dbenv *);
int check_sql_access(struct sqlthdstate *, struct sqlclntstate *);
int check_user_password(struct sqlclntstate *);

/* Validate write access to database pointed by cursor pCur */
int access_control_check_sql_write(struct BtCursor *, struct sql_thread *);
/* Validate read access to database pointed by cursor pCur */
int access_control_check_sql_read(struct BtCursor *pCur, struct sql_thread *thd, char *rscName);

int access_control_check_write(struct ireq *iq, tran_type *trans, int *bdberr);
int access_control_check_read(struct ireq *iq, tran_type *trans, int *bdberr);

/* Returns the current user for the session */
char *get_current_user(struct sqlclntstate *clnt);

/* Reset user struct */
void reset_user(struct sqlclntstate *clnt);

#endif /* !__INCLUDED_DB_AUTH_H */
