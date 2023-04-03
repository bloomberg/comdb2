/*
   Copyright 2020 Bloomberg Finance L.P.

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

#ifndef _STRING_REF_H
#define _STRING_REF_H

/* String reference class:
 *
 * Allows for multiple pointers to point to the same 'str' object
 * by means of counting references to 'this' object.
 *
 * NOTE: avoid assigning an object of this class to another explicitly,
 * as that can result in violating the correct counting! 
 * Instead, use transfer_ref(), or use get_ref() which increments the counter
 * and returns a reference to 'this'.
 *
 * NOTE: One use case of this class is to ref count object passed to another thread, in
 * which case: when passing a pointer to this class to a function or to another thread,
 * it has to be clear that only one of the copies should ultimately call put_ref().
 * For instance, in the case of passing to a thread, the thread takes over ownership
 * of the pointer (and needs to put_ref() accordingly).
 *
 * Releasing the reference should be only done via put_ref() which will free the object
 * if reference has reached count of 0. Notice that no other pointer to 'this' should
 * exist when correctly used because this is the last put_ref() and no other pointer
 * points to this object.
 *
 */

struct string_ref;

struct string_ref * create_string_ref_internal(const char *str, const char *func, int line);
#define create_string_ref(str) ({ struct string_ref *r; do { r = create_string_ref_internal(str, __func__, __LINE__); } while(0); r; })
struct string_ref * get_ref(struct string_ref *ref);
void put_ref(struct string_ref **ref);
void transfer_ref(struct string_ref **from, struct string_ref **to);
const char *string_ref_cstr(struct string_ref *ref);
size_t string_ref_len(struct string_ref *ref);
int all_string_references_cleared();

#endif
