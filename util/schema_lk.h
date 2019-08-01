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
#include "logmsg.h"

void schema_init_held(void);
void schema_term_held(void);

int schema_is_global_db_int(void *dbenv);
int schema_read_held_int(const char *file, const char *func, int line);
int schema_write_held_int(const char *file, const char *func, int line);

void dump_schema_lk(FILE *out);

#define schema_is_global_db(a) schema_is_global_db_int((a))

#define schema_read_held_lk() do {                                \
  if (!schema_read_held_int(__FILE__, __func__, __LINE__) &&      \
      !schema_write_held_int(__FILE__, __func__, __LINE__)) {     \
    logmsg(LOGMSG_FATAL, "SCHEMA LOCK NOT HELD: %s:%s:%d (%p)\n", \
           __FILE__, __func__, __LINE__, (void *)pthread_self()); \
    abort();                                                      \
  }                                                               \
} while (0)

#define schema_write_held_lk() do {                                     \
  if (!schema_write_held_int(__FILE__, __func__, __LINE__)) {           \
    logmsg(LOGMSG_FATAL, "SCHEMA WRITE LOCK NOT HELD: %s:%s:%d (%p)\n", \
           __FILE__, __func__, __LINE__, (void *)pthread_self());       \
    abort();                                                            \
  }                                                                     \
} while (0)

#define maybe_schema_read_held_lk(a) do {                  \
  if (schema_is_global_db((a))) { schema_read_held_lk(); } \
} while (0)

#define maybe_schema_write_held_lk(a) do {                  \
  if (schema_is_global_db((a))) { schema_write_held_lk(); } \
} while (0)

#else
#define schema_is_global_db(a) (0)
#define schema_read_held_lk()
#define schema_write_held_lk()
#define maybe_schema_read_held_lk(a)
#define maybe_schema_write_held_lk(a)
#endif

#define rdlock_schema_lk() rdlock_schema_int(__FILE__, __func__, __LINE__)
void rdlock_schema_int(const char *file, const char *func, int line);

#define tryrdlock_schema_lk() tryrdlock_schema_int(__FILE__, __func__, __LINE__)
int tryrdlock_schema_int(const char *file, const char *func, int line);

#define unlock_schema_lk() unlock_schema_int(__FILE__, __func__, __LINE__)
void unlock_schema_int(const char *file, const char *func, int line);

#define wrlock_schema_lk() wrlock_schema_int(__FILE__, __func__, __LINE__)
void wrlock_schema_int(const char *file, const char *func, int line);

#define maybe_rdlock_schema_lk_for_db(a) \
maybe_rdlock_schema_lk_for_db_int((a), __FILE__, __func__, __LINE__)
void maybe_rdlock_schema_lk_for_db_int(void *dbenv, const char *file,
                                       const char *func, int line);

#define maybe_wrlock_schema_lk_for_db(a) \
maybe_wrlock_schema_lk_for_db_int((a), __FILE__, __func__, __LINE__)
void maybe_wrlock_schema_lk_for_db_int(void *dbenv, const char *file,
                                       const char *func, int line);

#define maybe_unlock_schema_lk_for_db(a) \
maybe_unlock_schema_lk_for_db_int((a), __FILE__, __func__, __LINE__)
void maybe_unlock_schema_lk_for_db_int(void *dbenv, const char *file,
                                       const char *func, int line);

#endif
