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

#ifndef INCLUDED_COMPILE_TIME_ASSERT
#define INCLUDED_COMPILE_TIME_ASSERT

/* See compile_time_assert.c for a unit test. */

/**
 * Compile time assertions are a wonderful way to insure
 * correctness BEFORE moving a task to production at
 * no cost to efficiency.  One place, for instance, that I
 * have used them is to insure that the size of a struct
 * that MUST be a particular size because of transport
 * or storage size in fact is that size.  If you
 * are using C++, this concept can be carried further
 * using some cool template tricks.
 */

/* Assert (always at compile time, with no run-time cost) that 'expression' is
   nonzero. 'name' must be unique within the scope, but is otherwise unused. */
#define BB_COMPILE_TIME_ASSERT(name, expression)                               \
    struct COMPILE_TIME_##name##_ASSERT {                                      \
        char assertion[(expression) ? 1 : -1];                                 \
    }

/* Assert (where supported, and always at compile time, with no run-time cost)
   that the argument is an array with a locally defined size. Compiling with
   GCC, this takes advantage of GCC builtins: given 'int a[5]; int *b;',
   'typeof(a)' returns 'int[5]', which is not compatible with 'typeof(&a[0])' as
   that returns 'int*'; whereas, 'typeof(b)' and 'typeof(&b[0])' both return
   'int*', which are compatible, causing the assertion to fail. */
#ifdef __cplusplus
/* Could use proper C++ method here? */
#define BB_COMPILE_TIME_ASSERT_SIZED_ARRAY_AND_RETURN_SIZE_T_ZERO(a) ((size_t)0)
#else
#ifdef __GNUC__
#define BB_COMPILE_TIME_ASSERT_SIZED_ARRAY_AND_RETURN_SIZE_T_ZERO(a)           \
    BB_COMPILE_TIME_ASSERT_AND_RETURN_SIZE_T_ZERO(                             \
        !__builtin_types_compatible_p(typeof(a), typeof(&a[0])))
#else
#define BB_COMPILE_TIME_ASSERT_SIZED_ARRAY_AND_RETURN_SIZE_T_ZERO(a) ((size_t)0)
#endif
#endif

/* Assert (always at compile time, with no run-time cost) that 'e' is nonzero,
   but also produce a result (of value 0 and type size_t), so this macro can be
   expanded e.g. in a structure initializer (or whereever else comma expressions
   aren't permitted). */
#define BB_COMPILE_TIME_ASSERT_AND_RETURN_SIZE_T_ZERO(e)                       \
    (sizeof(char[1 - 2 * !(e)]) - 1)

#endif
