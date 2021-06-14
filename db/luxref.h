/*
   Copyright 2021, Bloomberg Finance L.P.

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

int luxref_init(tran_type *tran);
int luxref_add(tran_type *tran, struct dbtable *tbl);
int luxref_del(tran_type *tran, struct dbtable *tbl);
int luxref_hash_del(struct dbtable *tbl);
int luxref_hash_add(struct dbtable *tbl);
int luxref_rev_find(struct dbtable *tbl);
struct dbtable *luxref_find(int luxref);
void luxref_deinit();
void luxref_dump_info();
