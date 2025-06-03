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

#ifndef INCLUDE_SC_LUA_H
#define INCLUDE_SC_LUA_H

int dump_spfile(const char *file);
int read_spfile(const char *file);
struct ireq;
int do_add_sp(struct schema_change_type *, struct ireq *);
int do_del_sp(struct schema_change_type *sc, struct ireq *);
int do_default_sp(struct schema_change_type *, struct ireq *);
int do_show_sp(struct schema_change_type *sc, struct ireq *);

int finalize_add_sp(struct schema_change_type *sc);
int finalize_del_sp(struct schema_change_type *sc);
int finalize_default_sp(struct schema_change_type *sc);
int do_lua_sfunc(struct schema_change_type *, struct ireq *iq);
int do_lua_afunc(struct schema_change_type *, struct ireq *iq);

int reload_lua_sfuncs();
int reload_lua_afuncs();

int finalize_lua_sfunc(struct schema_change_type *unused);
int finalize_lua_afunc(struct schema_change_type *unused);

#endif
