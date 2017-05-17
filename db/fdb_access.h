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

/**
 * Access control for remote access
 *
 */

#ifndef _FDB_ACCESS_H_
#define _FDB_ACCESS_H_

/**
 * REMOTE ACCESS FLAGS
 *
 */
enum fdb_access_type {
    ACCESS_REMOTE_INV = 0,
    ACCESS_REMOTE_READ = 1,
    ACCESS_REMOTE_WRITE = 2,
    ACCESS_REMOTE_EXEC = 4
};

/**
 *   Create additional permissions of type "type" for client remote access
 *control
 *
 */
int fdb_access_control_create(struct sqlclntstate *clnt, char *str);

/**
 * Destroy remote access control for a client
 *
 */
int fdb_access_control_destroy(struct sqlclntstate *clnt);

/**
 * Check access control for this cursor
 * Returns -1 if access control enabled and access denied
 *         0 otherwise
 *
 */
int fdb_access_control_check(fdb_access_t *acc, const char *dbname,
                             const char *tblname, enum fdb_access_type type);

#endif
