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
 * Access control for remote access to a DB via a whitelist
 *
 */

#ifndef _FDB_WHITELIST_H_
#define _FDB_WHITELIST_H_

/*
 * Add dbname to whitelist
 */
int fdb_add_dbname_to_whitelist(const char *dbname);

/*
 * Check if dbname is in whitelist
 * if whitelist is null, then all dbs are allowed to query us
 */
int fdb_is_dbname_in_whitelist(const char *dbname);

int fdb_del_dbname_from_whitelist(const char *dbname);
void fdb_dump_whitelist();

#endif
