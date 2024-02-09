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

#define make_rwlock_prototypes(lock)                                                                        \
int have_##lock##_lock(void);                                                                               \
void debug_##lock##_enable(void);                                                                           \
void debug_##lock##_disable(void);                                                                          \
void verbose_##lock##_enable(void);                                                                         \
void verbose_##lock##_disable(void);                                                                        \
void rdlock_##lock##_int(const char *file, const char *func, int line);                                     \
int tryrdlock_##lock##_int(const char *file, const char *func, int line);                                   \
int trywrlock_##lock##_int(const char *file, const char *func, int line);                                   \
void unlock_##lock##_int(const char *file, const char *func, int line);                                     \
void wrlock_##lock##_int(const char *file, const char *func, int line);                                     \
void assert_wrlock_##lock##_int(const char *file, const char *func, int line);                              \
void assert_rdlock_##lock##_int(const char *file, const char *func, int line);                              \
void assert_lock_##lock##_int(const char *file, const char *func, int line);                                \
void assert_no_##lock##_int(const char *file, const char *func, int line);                                  \

make_rwlock_prototypes(schema);
make_rwlock_prototypes(views);

/* Can't have a define inside of a define unfortunately */
#define rdlock_schema_lk() rdlock_schema_int(__FILE__, __func__, __LINE__)
#define tryrdlock_schema_lk() tryrdlock_schema_int(__FILE__, __func__, __LINE__)
#define unlock_schema_lk() unlock_schema_int(__FILE__, __func__, __LINE__)
#define wrlock_schema_lk() wrlock_schema_int(__FILE__, __func__, __LINE__)
#define assert_wrlock_schema_lk() assert_wrlock_schema_int(__FILE__, __func__, __LINE__)
#define assert_rdlock_schema_lk() assert_rdlock_schema_int(__FILE__, __func__, __LINE__)
#define assert_lock_schema_lk() assert_lock_schema_int(__FILE__, __func__, __LINE__)
#define assert_no_schema_lk() assert_no_schema_int(__FILE__, __func__, __LINE__)

#define rdlock_views_lk() rdlock_views_int(__FILE__, __func__, __LINE__)
#define tryrdlock_views_lk() tryrdlock_views_int(__FILE__, __func__, __LINE__)
#define trywrlock_views_lk() trywrlock_views_int(__FILE__, __func__, __LINE__)
#define unlock_views_lk() unlock_views_int(__FILE__, __func__, __LINE__)
#define wrlock_views_lk() wrlock_views_int(__FILE__, __func__, __LINE__)
#define assert_wrlock_views_lk() assert_wrlock_views_int(__FILE__, __func__, __LINE__)
#define assert_rdlock_views_lk() assert_rdlock_views_int(__FILE__, __func__, __LINE__)
#define assert_lock_views_lk() assert_lock_views_int(__FILE__, __func__, __LINE__)
#define assert_no_views_lk() assert_no_views_int(__FILE__, __func__, __LINE__)

#endif
