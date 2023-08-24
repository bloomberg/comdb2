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
#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#include "../berkdb/dbinc/queue.h"
#include "cson.h"

/* cson::glue::json1 */

enum {
    SQLITE_INTEGER = 1,
    SQLITE_FLOAT,
    SQLITE_BLOB,
    SQLITE_NULL,
    SQLITE_TEXT,
    SQLITE_DATETIME,
    SQLITE_DATETIMEUS,
    SQLITE_INTERVAL_YM,
    SQLITE_INTERVAL_DS,
    SQLITE_INTERVAL_DSUS,
    SQLITE_DECIMAL,

    SQLITE_OK = 0,
    SQLITE_NOMEM = 7,

    SQLITE_UTF8 = 1,

    SQLITE_DETERMINISTIC = 0x800,
};

#define i64 int64_t
#define sqlite3 void
#define sqlite3_context cson_kvp
#define sqlite3_int64 int64_t
#define sqlite3_uint64 uint64_t
#define sqlite3_value cson_value

#define sqlite3_free free
#define sqlite3_malloc malloc
#define sqlite3_malloc64(a) malloc((size_t)a)
#define sqlite3_realloc64(a, b) realloc(a, (size_t)b)

#define sqlite3_value_bytes cson__value_bytes
#define sqlite3_value_subtype cson__value_subtype
#define sqlite3_value_text cson__value_text
#define sqlite3_value_type cson__value_type

#define sqlite3_result_double cson__result_double
#define sqlite3_result_int cson__result_int
#define sqlite3_result_int64 cson__result_int64
#define sqlite3_result_null cson__result_null
#define sqlite3_result_subtype cson__result_subtype
#define sqlite3_result_text cson__result_text
#define sqlite3_result_text64 cson__result_text64

#define SQLITE_STATIC cson__not_reached_alloc
#define SQLITE_TRANSIENT cson__not_reached_alloc
#define sqlite3_aggregate_context(a, ...) cson__not_reached_void_ptr((intptr_t) a, __VA_ARGS__)
#define sqlite3_create_function(a, ...) (intptr_t)cson__not_reached_void_ptr((intptr_t) a, __VA_ARGS__)
#define sqlite3_get_auxdata(a, ...) cson__not_reached_void_ptr((intptr_t) a, __VA_ARGS__)
#define sqlite3_mprintf(a, ...) cson__not_reached_void_ptr((intptr_t) a, __VA_ARGS__)
#define sqlite3_result_error(...) cson__not_reached_void()
#define sqlite3_result_error_nomem(...) cson__not_reached_void()
#define sqlite3_result_value(...) cson__not_reached_void()
#define sqlite3_set_auxdata(...) cson__not_reached_void()
#define sqlite3_user_data(a) cson__not_reached_void_ptr((intptr_t) a)
#define sqlite3_vsnprintf(a, ...) cson__not_reached_void_ptr((intptr_t) a, __VA_ARGS__)

static int cson__value_bytes(cson_value *);
static int cson__value_subtype(cson_value *);
static int cson__value_type(cson_value *);
static char *cson__value_text(cson_value *);

static void cson__result_double(cson_kvp *, double);
static void cson__result_int(cson_kvp *, int);
static void cson__result_int64(cson_kvp *, int64_t);
static void cson__result_null(cson_kvp *);
static void cson__result_subtype(cson_kvp *, unsigned int);
static void cson__result_text(cson_kvp *, const char *, int , void (*)(void *));
static void cson__result_text64(cson_kvp *, const char *, uint64_t, void (*)(void *), unsigned char);

static void cson__not_reached_alloc(void *);
static void cson__not_reached_void(void);
static void *cson__not_reached_void_ptr(intptr_t, ...);

/* Evil: Need to borrow SQLite's JSON implementation. We also, want, just the
 * implementation. We don't want sqlite3.h, sqliteInt.h, ... */
#define SQLITE3_H
#define SQLITEINT_H
#define SQLITE_API
#define SQLITE_BUILDING_FOR_COMDB2
#define SQLITE_CORE
#define SQLITE_ENABLE_JSON1
#define SQLITE_EXTENSION_INIT1
#define SQLITE_OMIT_VIRTUALTABLE
#define SQLITE_OMIT_WINDOWFUNC
#define sqlite3Json1Init cson__sqlite3Json1Init
#include "../sqlite/ext/misc/json1.c"

/* List of cson_values created for $[key] or $.key */
LIST_HEAD(get_list, cson_get);

/* List of key names added to cson_object */
LIST_HEAD(key_list, cson_key);

struct cson_array {
    JsonString str;
    cson_value *value;
    struct get_list get_list;
};
struct cson_object {
    cson_value *value;
    struct key_list key_list;
    struct get_list get_list;
};
struct cson_value {
    int sql_type;
    int sub_type;
    int value_bytes;
    char *value_text;
    char value_buf[128];
    int modified;
    JsonString output;
    struct {
        cson_value **slots;
        int capacity;
        int used;
    } replace;
    JsonParse parse;
    union {
        int64_t num;
        double dbl;
        cson_object obj;
        cson_array arr;
    } u;
};
typedef struct cson_key {
    LIST_ENTRY(cson_key) entry;
    char buf[0];
} cson_key;
typedef struct cson_get {
    LIST_ENTRY(cson_get) entry;
    cson_value *value;
} cson_get;
struct cson_kvp {
    char *key;
    int val_type;
    cson_value *val;
};
static void cson__not_reached_alloc(void *arg)
{
    abort();
}
static void cson__not_reached_void(void)
{
    abort();
}
static void *cson__not_reached_void_ptr(intptr_t arg, ...)
{
    raise(SIGABRT);
    return NULL;
}
static int cson__value_type(cson_value *val)
{
    if (val->sql_type == 0) abort();
    return val->sql_type;
}
static int cson__value_subtype(cson_value *val)
{
    return val->sub_type;
}
static int cson__value_bytes(cson_value *val)
{
    return val->value_bytes;
}
static char *cson__value_text(cson_value *val)
{
    return val->value_text;
}

static void cson__result_double(cson_kvp *kvp, double rVal)
{
    kvp->val = cson_value_new_double(rVal);
}
static void cson__result_int(cson_kvp *kvp, int iVal)
{
    kvp->val = cson_value_new_bool(iVal);
}
static void cson__result_int64(cson_kvp *kvp, int64_t iVal)
{
    kvp->val = cson_value_new_integer(iVal);
}
static void cson__result_null(cson_kvp *kvp)
{
    kvp->val = cson_value_null();
}
static void cson__result_subtype(cson_kvp *kvp, unsigned int eSubtype)
{
}
static void cson__result_text(cson_kvp *kvp, const char *z, int n,
                                void (*xDel)(void *))
{
    if (!kvp->key) {
        kvp->key = (xDel == free) ? (char *)z : strndup(z, n);
        return;
    }
    if (kvp->val_type == JSON_OBJECT || kvp->val_type == JSON_ARRAY) {
        cson_parse_string(&kvp->val, z, n);
    } else {
        kvp->val = cson_value_new_string(z, n);
    }
    if (xDel == free)
        free((void *)z);
}
static void cson__result_text64(cson_kvp *kvp, const char *z,
                                  uint64_t n, void (*xDel)(void *),
                                  unsigned char enc)
{
    cson__result_text(kvp, z, n, xDel);
}

static void cson__reset_slots(cson_value *v)
{
    for (int i = 0; i < v->replace.used; ++i) {
        cson_value_free(v->replace.slots[i]);
    }
    v->replace.used = 0;
}
static int cson__type(cson_value const *val)
{
    if (val->sub_type == 'J') {
        return val->parse.aNode[0].eType;
    }
    return -1;
}
static void cson__free_get_list(struct get_list *get_list)
{
    cson_get *get, *tmp_get;
    LIST_FOREACH_SAFE(get, get_list, entry, tmp_get) {
        LIST_REMOVE(get, entry);
        cson_value_free(get->value);
        free(get);
    }
}
static void cson__reset(cson_value *val)
{
    jsonReset(&val->output);
    cson__reset_slots(val);
    free(val->replace.slots);
    val->replace.used = 0;
    val->replace.capacity = 0;
    val->replace.slots = NULL;

    if (val->value_text != val->value_buf) {
        free(val->value_text);
        val->value_text = NULL;
        val->value_bytes = 0;
    }

    if (!val->sub_type) {
        return;
    }
    int type = cson__type(val);
    if (type == JSON_OBJECT) {
        cson_object *obj = cson_value_get_object(val);
        cson__free_get_list(&obj->get_list);
        cson_key *key, *tmp_key;
        LIST_FOREACH_SAFE(key, &obj->key_list, entry, tmp_key) {
            LIST_REMOVE(key, entry);
            free(key);
        }
    } else if (type == JSON_ARRAY) {
        cson__free_get_list(&val->u.arr.get_list);
        jsonReset(&val->u.arr.str);
    }
    jsonParseReset(&val->parse);
}
static char *strncpy0(char *dest, const char *src, int n)
{
    strncpy(dest, src, n);
    dest[n] = 0;
    return dest;
}
static cson_value *cson__parse(cson_value *val, const char *json_in, int n)
{
    cson__reset(val);
    char *json = n < sizeof(val->value_buf)
                     ? strncpy0(val->value_buf, json_in, n)
                     : strndup(json_in, n);
    JsonParse *parse = &val->parse;
    if (jsonParse(parse, NULL, json)) {
        if (json != val->value_buf) {
            free(json);
        }
        return NULL;
    }
    if (parse->nNode == 0)
        abort();
    val->modified = 0;
    val->sql_type = SQLITE_TEXT;
    val->sub_type = JSON_SUBTYPE;
    val->value_text = json;
    val->value_bytes = n;
    switch (cson__type(val)) {
    case JSON_ARRAY:
       val->u.arr.value = val;
       break;
    case JSON_OBJECT:
       val->u.obj.value = val;
       break;
    }
    return val;
}
static cson_value *cson__get_value(JsonNode *node)
{
    cson_kvp kvp = {0};
    kvp.key = (void *)-1;
    kvp.val_type = node->eType;
    jsonReturn(node, &kvp, 0);
    return kvp.val;
}
static cson_get *cson__get(cson_value *val, char *path)
{
    JsonParse *parse = &val->parse;
    JsonNode *node = jsonLookup(parse, path, 0, 0);
    if (!node) {
        return NULL;
    }
    cson_get *ret = calloc(1, sizeof(cson_get));
    JsonString s;
    jsonInit(&s, NULL);
    jsonRenderNode(node, &s, 0);
    if (node->eType == JSON_ARRAY || node->eType == JSON_OBJECT) {
        ret->value = calloc(1, sizeof(cson_value));
        cson__parse(ret->value, s.zBuf, s.nUsed);
    } else {
        ret->value = cson__get_value(node);
    }
    jsonReset(&s);
    return ret;
}
static void cson__grow_slots(cson_value *val)
{
    if (val->replace.used == val->replace.capacity) {
        if (val->replace.capacity == 0) {
            val->replace.capacity = 10;
        } else {
            val->replace.capacity *= 10;
        }
        size_t sz = sizeof(val->replace.slots[0]) * val->replace.capacity;
        val->replace.slots = realloc(val->replace.slots, sz);
    }
}
static void cson__output(cson_value *val)
{
    jsonReset(&val->output);
    jsonRenderNode(val->parse.aNode, &val->output, val->replace.slots);
    if (cson__type(val) == JSON_ARRAY && val->u.arr.str.nUsed) {
        // process appended values
        cson_array *arr = cson_value_get_array(val);
        JsonString *suffix = &arr->str;
        --val->output.nUsed; // remove trailing ]
        jsonAppendSeparator(&val->output);
        jsonAppendRaw(&val->output, suffix->zBuf, suffix->nUsed);
        jsonAppendChar(&val->output, ']');
        jsonReset(suffix);
    }
}
static void cson__render(cson_value *val)
{
    if (!val->modified) return;
    cson__output(val);
    JsonString tmp = val->output;
    if (tmp.bStatic) tmp.zBuf = tmp.zSpace;
    jsonInit(&val->output, NULL);
    cson__parse(val, tmp.zBuf, tmp.nUsed);
    jsonReset(&tmp);
}
static int cson__set(cson_value *val, char *path, cson_value *v)
{
    JsonParse *parse = &val->parse;
    int append = 0;
    JsonNode *node = jsonLookup(parse, path, v ? &append : NULL, NULL);
    if (parse->nErr || !node) {
        return -1;
    }
    val->modified = 1;
    if (v == NULL) {
        node->jnFlags |= JNODE_REMOVE;
        return 0;
    }
    if (node->jnFlags & JNODE_REPLACE) {
        cson_value *old = val->replace.slots[node->u.iReplace];
        cson_value_free(old);
    } else {
        cson__grow_slots(val);
        node->jnFlags |= JNODE_REPLACE;
        node->u.iReplace = val->replace.used++;
    }
    val->replace.slots[node->u.iReplace] = v;
    int type = cson__type(v);
    if (type != JSON_OBJECT && type != JSON_ARRAY) {
        return 0;
    }
    cson__render(v);
    return 0;
}


/****************************************/
/** Implementation of public interface **/
/****************************************/
cson_value *cson_value_new_object(void)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_TEXT;
    return cson__parse(val, "{}", 2);
}
cson_object *cson_new_object(void)
{
    return cson_value_get_object(cson_value_new_object());
}
char cson_value_is_object(cson_value const *v)
{
    return cson__type(v) == JSON_OBJECT;
}
cson_object *cson_value_get_object(cson_value *val)
{
    return &val->u.obj;
}
int cson_value_fetch_object(cson_value *val, cson_object **obj)
{
    *obj = cson_value_get_object(val);
    return 0;
}
cson_value *cson_object_value(cson_object *s)
{
    return s->value;
}
int cson_object_set(cson_object *obj, char const *key, cson_value *v)
{
    size_t key_len = strlen(key) + 3;
    struct cson_key *cson_key = malloc(sizeof(struct cson_key) + key_len);
    sprintf(cson_key->buf, "$.%s", key);
    LIST_INSERT_HEAD(&obj->key_list, cson_key, entry);
    return cson__set(obj->value, cson_key->buf, v);
}
int cson_object_unset(cson_object *obj, char const *key)
{
    return cson_object_set(obj, key, NULL);
}
cson_value *cson_object_get(cson_object *obj, char const *key)
{
    cson__render(obj->value);
    char path[strlen(key) + 3];
    sprintf(path, "$.%s", key);
    cson_get *get = cson__get(obj->value, path);
    if (!get) {
        return NULL;
    }
    LIST_INSERT_HEAD(&obj->get_list, get, entry);
    return get->value;
}
cson_value *cson_value_new_array(void)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_TEXT;
    cson__parse(val, "[]", 2);
    cson_array *arr = &val->u.arr;
    jsonInit(&arr->str, NULL);
    return val;
}
char cson_value_is_array(cson_value const *v)
{
    return cson__type(v) == JSON_ARRAY;
}
cson_array *cson_value_get_array(cson_value *val)
{
    return &val->u.arr;
}
int cson_array_set(cson_array *ar, unsigned int ndx, cson_value *v)
{
    cson__render(ar->value);
    char path[256];
    sprintf(path, "$[%d]", ndx);
    cson__set(ar->value, path, v);
    return 0;
}
cson_value *cson_array_get(cson_array *ar, unsigned int pos)
{
    cson__render(ar->value);
    char path[256];
    sprintf(path, "$[%d]", pos);
    cson_get *get = cson__get(ar->value, path);
    if (!get) {
        return NULL;
    }
    LIST_INSERT_HEAD(&ar->get_list, get, entry);
    return get->value;
}
unsigned int cson_array_length_get(cson_array const *ar)
{
    cson__render(ar->value);
    unsigned int n = 0;
    JsonNode *node = ar->value->parse.aNode;
    for (int i = 1; i <= node->n; n++) {
        i += jsonNodeSize(&node[i]);
    }
    return n;
}
int cson_array_append(cson_array *ar, cson_value *v)
{
    ar->value->modified = 1;
    if (v->sub_type) {
        cson__render(v);
    }
    jsonAppendSeparator(&ar->str);
    jsonAppendValue(&ar->str, v);
    cson_value_free(v);
    return 0;
}
cson_value *cson_value_new_integer(cson_int_t v)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_INTEGER;
    val->u.num = v;
    val->value_bytes = snprintf(val->value_buf, sizeof(val->value_buf), "%" PRId64, v);
    if (val->value_bytes < sizeof(val->value_buf)) {
        val->value_text = val->value_buf;
    } else {
        val->value_text = malloc(val->value_bytes + 1);
        sprintf(val->value_text, "%" PRId64, v);
    }
    return val;
}
char cson_value_is_integer(cson_value const *v)
{
    if (v->sql_type == SQLITE_INTEGER) return 1;
    return cson__type(v) == JSON_INT;
}
cson_int_t cson_value_get_integer(cson_value const *val)
{
    return val->u.num;
}
int cson_value_fetch_integer(cson_value const *val, cson_int_t *v)
{
    *v = cson_value_get_integer(val);
    return 0;
}
cson_value *cson_new_int(cson_int_t v)
{
    return cson_value_new_integer(v);
}
cson_value *cson_value_new_double(cson_double_t v)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_FLOAT;
    val->u.dbl = v;
    int neg;
    if (isnan(v)) {
        val->value_bytes = snprintf(val->value_buf, sizeof(val->value_buf), "nan");
    } else if ((neg = isinf(v)) != 0) {
        val->value_bytes = snprintf(val->value_buf, sizeof(val->value_buf), "%sinf", neg < 0 ? "-" : "");
    } else {
        val->value_bytes = snprintf(val->value_buf, sizeof(val->value_buf), "%.15g", v);
    }
    if (val->value_bytes < sizeof(val->value_buf)) {
        val->value_text = val->value_buf;
    } else {
        val->value_text = malloc(val->value_bytes + 1);
        sprintf(val->value_text, "%.15g", v);
    }
    return val;
}
char cson_value_is_double(cson_value const *v)
{
    if (cson_value_is_integer(v)) return 1;
    if (v->sql_type == SQLITE_FLOAT) return 1;
    return cson__type(v) == JSON_REAL;
}
cson_double_t cson_value_get_double(cson_value const *val)
{
    if (cson_value_is_integer(val))
        return val->u.num;
    return val->u.dbl;
}
int cson_value_fetch_double(cson_value const *val, cson_double_t *v)
{
    *v = cson_value_get_double(val);
    return 0;
}
cson_value *cson_new_double(cson_double_t v)
{
    return cson_value_new_double(v);
}
cson_value *cson_value_new_string(char const *str, unsigned int n)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_TEXT;
    val->value_text = n < sizeof(val->value_buf) - 1
                          ? strncpy0(val->value_buf, str, n)
                          : strndup(str, n);
    val->value_bytes = n;
    return val;
}
char cson_value_is_string(cson_value const *v)
{
    if (v->sub_type) {
        return cson__type(v) == JSON_STRING;
    }
    return v->sql_type == SQLITE_TEXT;
}
cson_string *cson_value_get_string(cson_value const *val)
{
    return (char *)val->value_text;
}
char *cson_value_get_cstr(cson_value *val)
{
    if (!cson_value_is_string(val))
        return NULL;
    return cson_value_get_string(val);
}
int cson_value_fetch_string(cson_value *val, cson_string **str)
{
    *str = cson_value_get_string(val);
    return 0;
}
char const *cson_string_cstr(cson_string const *str)
{
    return str;
}
cson_value *cson_value_null(void)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_NULL;
    val->value_text = strcpy(val->value_buf, "null");
    val->value_bytes = 4;
    return val;
}
char cson_value_is_null(cson_value const *v)
{
    if (v->sql_type == SQLITE_NULL) return 1;
    return cson__type(v) == JSON_NULL;
}
cson_value *cson_value_new_bool(char v)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    val->sql_type = SQLITE_TEXT;
    return v ? cson__parse(val, "true", 4) : cson__parse(val, "false", 5);
}
char cson_value_is_bool(cson_value const *v)
{
    int type = cson__type(v);
    return type == JSON_TRUE || type == JSON_FALSE;
}
char cson_value_get_bool(cson_value const *val)
{
    return cson__type(val) == JSON_TRUE;
}
int cson_output_FILE(cson_value *val, FILE *dest)
{
    if (val->sub_type && val->modified) {
        cson__output(val);
        fwrite(val->output.zBuf, val->output.nUsed, 1, dest);
    } else {
        fwrite(val->value_text, val->value_bytes, 1, dest);
    }
    fputc('\n', dest);
    return 0;
}
int cson_output_buffer(cson_value *val, cson_buffer *buf)
{
    if (val->sub_type && val->modified) {
        cson__output(val);
        buf->mem = val->output.zBuf;
        buf->used = val->output.nUsed;
    } else {
        buf->mem = val->value_text;
        buf->used = val->value_bytes;
    }
    return 0;
}
int cson_output(cson_value *val, cson_data_dest_f f, void *arg)
{
    int rc;
    if (val->sub_type) {
        cson__output(val);
        rc = f(arg, val->output.zBuf, val->output.nUsed);
    } else {
        rc = f(arg, val->value_text, val->value_bytes);
    }
    if (rc == 0) {
        rc = f(arg, "\n", 1);
    }
    return rc;
}
void cson_value_free(cson_value *v)
{
    cson__reset(v);
    free(v);
}
void cson_free_value(cson_value *v)
{
    cson_value_free(v);
}
int cson_parse_string(cson_value **out, char const *src, unsigned int len)
{
    cson_value *val = calloc(1, sizeof(cson_value));
    if (cson__parse(val, src, len)) {
        *out = val;
        return 0;
    }
    free(val);
    return -1;
}
int cson_object_iter_init(cson_object *obj, cson_object_iterator *iter)
{
    iter->kv = calloc(1, sizeof(cson_kvp));
    iter->obj = obj;
    cson__render(obj->value);
    iter->i = 1;
    iter->end = obj->value->parse.aNode[0].n;
    return 0;
}
cson_kvp *cson_object_iter_next(cson_object_iterator *iter)
{
    cson_kvp *kvp = iter->kv;
    free(kvp->key);
    kvp->key = NULL;
    if (kvp->val) {
        cson_value_free(kvp->val);
        kvp->val = NULL;
    }
    if (iter->i >= iter->end) {
        free(iter->kv);
        return NULL;
    }
    JsonParse *parse = &iter->obj->value->parse;
    JsonNode *key  = &parse->aNode[iter->i];
    iter->i += jsonNodeSize(&parse->aNode[iter->i]);
    JsonNode *val = &parse->aNode[iter->i];
    iter->i += jsonNodeSize(&parse->aNode[iter->i]);
    jsonReturn(key, kvp, 0);
    kvp->val_type = val->eType;
    jsonReturn(val, kvp, 0);
    return iter->kv;
}
cson_string *cson_kvp_key(cson_kvp const *kvp)
{
    return kvp->key;
}
cson_value *cson_kvp_value(cson_kvp const *kvp)
{
    return kvp->val;
}
char const *cson_rc_string(int rc)
{
    return "malformed JSON";
}
