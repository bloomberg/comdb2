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

#ifndef INCLUDED_SP_H
#define INCLUDED_SP_H

struct Mem;
struct sqlthdstate;
struct sqlclntstate;
struct sqlite3_context;
struct trigger_reg;

struct spversion_t {
    int version_num;
    char *version_str;
};

int exec_procedure(struct sqlthdstate *, struct sqlclntstate *, char **err);
int exec_thread(struct sqlthdstate *, struct sqlclntstate *);
void *exec_trigger(struct trigger_reg *);
void close_sp(struct sqlclntstate *);

void lua_final(struct sqlite3_context *);
void lua_step(struct sqlite3_context *, int argc, struct Mem **argv);
void lua_func(struct sqlite3_context *, int argc, struct Mem **argv);
int db_verify_table_callback(void *v, const char *buf);

#endif
