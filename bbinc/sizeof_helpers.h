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

#ifndef INCLUDED_SIZEOF_HELPERS
#define INCLUDED_SIZEOF_HELPERS

/* See sizeof_helpers.c for a unit test. */

#include <compile_time_assert.h>
#include <fieldof_helpers.h>

/* Return the offset of the first byte after (following) a field within a
   structure type as a size_t. This includes alignment based on the type of the
   field itself, but not based on the structure type. E.g. given 'struct S {
   int64_t wide; int8_t narrow; };', offsetof_after(struct S, narrow) => 9. */
#define offsetof_after(type, fld) ((size_t)((&fieldof_type(type, fld)) + 1))

/* Return the size of a field in a structure type as a size_t. */
#define sizeof_field(type, fld) sizeof(fieldof_type(type, fld))

/* Return sizeof an array, which mustn't be a pointer, as a size_t. */
#define sizeof_array(a)                                                        \
    (sizeof(a) + BB_COMPILE_TIME_ASSERT_SIZED_ARRAY_AND_RETURN_SIZE_T_ZERO(a))

/* Return sizeof an array field within a type, which mustn't be a pointer, as a
   size_t. */
#define sizeof_array_field(type, a) sizeof_array(fieldof_type(type, a))

/* Return the number of (top-level) elements in an array, which mustn't be a
   pointer, as a size_t. */
#define countof_array(a)                                                       \
    ((sizeof(a) / sizeof((a)[0])) +                                            \
     BB_COMPILE_TIME_ASSERT_SIZED_ARRAY_AND_RETURN_SIZE_T_ZERO(a))

/* Return the number of (top-level) elements in an array field within a type,
   which mustn't be a pointer, as a size_t. */
#define countof_array_field(type, a) countof_array(fieldof_type(type, a))

#endif
