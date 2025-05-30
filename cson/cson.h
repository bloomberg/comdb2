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
#ifndef CSON_H
#define CSON_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>

typedef int64_t cson_int_t;
typedef double cson_double_t;
typedef char cson_string;
typedef size_t cson_size_t;

typedef struct cson_value cson_value;
typedef struct cson_object cson_object;
typedef struct cson_array cson_array;
typedef struct cson_buffer cson_buffer;
typedef struct cson_kvp cson_kvp;
typedef struct cson_object_iterator cson_object_iterator;
typedef int (*cson_data_dest_f)(void *, const void *, unsigned int);

struct cson_buffer {
    int used;
    void *mem;
};

struct cson_object_iterator {
    cson_object *obj;
    cson_kvp *kv;
    unsigned i;
    unsigned end;
};

char *cson_value_get_cstr(cson_value *);
char const *cson_rc_string(int rc);
char const *cson_string_cstr(cson_string const *str);
char cson_value_get_bool(cson_value const *);
char cson_value_is_array(cson_value const *);
char cson_value_is_bool(cson_value const *);
char cson_value_is_double(cson_value const *);
char cson_value_is_integer(cson_value const *);
char cson_value_is_null(cson_value const *);
char cson_value_is_object(cson_value const *);
char cson_value_is_string(cson_value const *);
cson_array *cson_value_get_array(cson_value *);
cson_double_t cson_value_get_double(cson_value const *);
cson_int_t cson_value_get_integer(cson_value const *);
cson_kvp *cson_object_iter_next(cson_object_iterator *iter);
cson_object *cson_new_object(void);
cson_object *cson_value_get_object(cson_value *);
cson_string *cson_kvp_key(cson_kvp const *kvp);
cson_string *cson_value_get_string(cson_value const *);
cson_value *cson_array_get(cson_array *, unsigned int pos);
cson_value *cson_kvp_value(cson_kvp const *kvp);
cson_value *cson_new_double(cson_double_t, int quote_strings);
cson_value *cson_new_int(cson_int_t);
cson_value *cson_object_get(cson_object *, char const *key);
cson_value *cson_object_value(cson_object *s);
cson_value *cson_value_new_array(void);
cson_value *cson_value_new_bool(char v);
cson_value *cson_value_new_double(cson_double_t v, int quote_strings);
cson_value *cson_value_new_integer(cson_int_t v);
cson_value *cson_value_new_object(void);
cson_value *cson_value_new_string(char const *str, unsigned int n);
cson_value *cson_value_new_blob(char *, size_t);
cson_value *cson_value_null(void);
int cson_array_append(cson_array *, cson_value *);
int cson_array_set(cson_array *, unsigned int ndx, cson_value *);
int cson_object_iter_init(cson_object *, cson_object_iterator *iter);
int cson_object_set(cson_object *, char const *key, cson_value *);
int cson_object_unset(cson_object *, char const *key);
int cson_output(cson_value *, cson_data_dest_f dest, void *destState);
int cson_output_FILE(cson_value *, FILE *dest);
int cson_output_buffer(cson_value *, cson_buffer *buf);
int cson_parse_string(cson_value **, const char *, unsigned int len);
int cson_value_fetch_double(cson_value const *, cson_double_t *);
int cson_value_fetch_integer(cson_value const *, cson_int_t *);
int cson_value_fetch_object(cson_value *, cson_object **);
int cson_value_fetch_string(cson_value *, cson_string **str);
unsigned int cson_array_length_get(cson_array const *);
void cson_free_value(cson_value *);
void cson_value_free(cson_value *);

#ifdef __cplusplus
}
#endif

#endif
