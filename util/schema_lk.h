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

#ifndef INCLUDED_SCHEMA_LK_H
#define INCLUDED_SCHEMA_LK_H

#include <locks_wrap.h>

#ifndef NDEBUG
#include <stdio.h>

#define schema_read_held_lk() schema_read_held_int(__FILE__, __func__, __LINE__)
int schema_read_held_int(const char *file, const char *func, int line);

#define schema_write_held_lk() schema_write_held_int(__FILE__, __func__, __LINE__)
int schema_write_held_int(const char *file, const char *func, int line);

void dump_schema_lk(FILE *out);
#endif

#define rdlock_schema_lk() rdlock_schema_int(__FILE__, __func__, __LINE__)
void rdlock_schema_int(const char *file, const char *func, int line);

#define tryrdlock_schema_lk() tryrdlock_schema_int(__FILE__, __func__, __LINE__)
int tryrdlock_schema_int(const char *file, const char *func, int line);

#define unlock_schema_lk() unlock_schema_int(__FILE__, __func__, __LINE__)
void unlock_schema_int(const char *file, const char *func, int line);

#define wrlock_schema_lk() wrlock_schema_int(__FILE__, __func__, __LINE__)
void wrlock_schema_int(const char *file, const char *func, int line);

#endif
