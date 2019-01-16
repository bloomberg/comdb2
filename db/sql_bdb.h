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

#ifndef _SQL_BDB_H_
#define _SQL_BDB_H_

#ifndef NDEBUG
void bdb_verify_tran_invariants(bdb_state_type *bdb_state, tran_type *tran, char *filename, int line, char *func);
#define BDB_VERIFY_TRAN_INVARIANTS(a,b) bdb_verify_tran_invariants((a), (tran_type *)(b), __FILE__, __LINE__, __func__)
#else
#define BDB_VERIFY_TRAN_INVARIANTS(a,b)
#endif

#endif /* _SQL_BDB_H_ */
