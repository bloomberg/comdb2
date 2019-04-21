/*
   Copyright 2019, Bloomberg Finance L.P.

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


#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include "logmsg.h"
#include "dyn_array.h"
#include "temptable.h"
#include "ix_return_codes.h"



static int
dyn_array_keyval_cmpr_asc(const void *p1, const void *p2, void *p3)
{
    const key_val_t *kv1 = p1;
    const key_val_t *kv2 = p2;
    dyn_array_t *arr = p3;
    char *buffer = arr->buffer;
    int res = 0;
    void *key1 = &buffer[kv1->key_start];
    void *key2 = &buffer[kv2->key_start];
    if (arr->compar) {
        res = arr->compar(NULL, kv1->key_len, key1, kv2->key_len, key2);
    }
    else {
        res = memcmp(key1, key2, kv1->key_len < kv2->key_len ? kv1->key_len : kv2->key_len);
    }
    return res;
}

void dyn_array_close(dyn_array_t *arr)
{
    if (arr->using_temp_table) {
        int bdberr;
        int rc;
        rc = bdb_temp_table_close_cursor(arr->bdb_env, arr->temp_table_cur, &bdberr);
        if (rc) abort();
        rc = bdb_temp_table_close(arr->bdb_env, arr->temp_table, &bdberr);
        if (rc) abort();
    }
    else {
        if (arr->kv) {
            assert(arr->capacity > 0);
            free(arr->kv);
        }
        if (arr->buffer) {
            assert(arr->buffer_capacity > 0);
            free(arr->buffer);
        }
    }
    memset(arr, 0, sizeof(dyn_array_t));
}

/* need to initialize properly with bdb_env and a comparator fuction
 * if you want this to spill to a temptable */
void dyn_array_init(dyn_array_t *arr, void *bdb_env)
{
    if (arr->capacity > 0)
        dyn_array_close(arr);
    arr->bdb_env = bdb_env;
}

void dyn_array_set_cmpr(dyn_array_t *arr,
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2))
{
    arr->compar = compar;
}

int dyn_array_sort(dyn_array_t *arr)
{
    if (arr->capacity == 0) {
        assert(arr->items == 0);
        return 0;
    }

    qsort_r(arr->kv, arr->items, sizeof(key_val_t), dyn_array_keyval_cmpr_asc, arr);
    return 0;
}

static inline struct temp_table *create_temp_table(dyn_array_t *arr)
{
    int bdberr = 0;
    struct temp_table *newtbl =
        (struct temp_table *)bdb_temp_table_create(arr->bdb_env, &bdberr);
    if (newtbl == NULL || bdberr != 0) {
        logmsg(LOGMSG_ERROR, "failed to create temp table err %d\n", bdberr);
        return NULL;
    }
    arr->temp_table = newtbl;
    bdb_temp_table_set_cmp_func(newtbl, arr->compar);
    arr->temp_table_cur = bdb_temp_table_cursor(arr->bdb_env, arr->temp_table, NULL, &bdberr);
    if (!arr->temp_table_cur)
        abort();
    return newtbl;
}


static inline void transfer_to_temp_table(dyn_array_t *arr)
{
    for (int i = 0; i < arr->items; i++) {
        //logmsg(LOGMSG_ERROR, "AZ: %d: ", i); 
        char *buffer = arr->buffer;
        key_val_t *kv = &arr->kv[i];
        void *key = &buffer[kv->key_start];
        void *data = &buffer[kv->data_start];
        printf("%d: %d %d\n", i, *(int *)key, kv->key_len);
        int bdberr;
        int rc = bdb_temp_table_insert(arr->bdb_env, arr->temp_table_cur,
                key, kv->key_len, data, kv->data_len, &bdberr);
        if (rc) abort();
    }
}

static inline int do_transfer(dyn_array_t *arr) 
{
    logmsg(LOGMSG_ERROR, "time to spill to temp table");
    assert(arr->using_temp_table == 0);
    assert(arr->temp_table == NULL);
    assert(arr->temp_table_cur == NULL);
    arr->using_temp_table = 1;
    arr->temp_table = create_temp_table(arr);
    if (!arr->temp_table)
        return 1;
    transfer_to_temp_table(arr);
    free(arr->kv);
    if(arr->buffer)
        free(arr->buffer);
    arr->kv = NULL;
    arr->buffer = NULL;
    arr->capacity = 0;
    arr->items = 0;
    arr->buffer_capacity = 0;
    arr->buffer_curr_offset = 0;
    return 0;
}

static inline int append_to_array(dyn_array_t *arr, void *key, int keylen, void *data, int datalen)
{
    if (arr->capacity == 0) {
        assert(arr->items == 0);
        arr->capacity = 512;
        arr->kv = malloc(sizeof(*arr->kv) * arr->capacity);
        arr->buffer_capacity = 16*1024;
        arr->buffer = malloc(arr->buffer_capacity);
        if (!arr->kv) return 1;
    }
    if (arr->items + 1 >= arr->capacity) {
        arr->capacity *= 2;
        void *n = realloc(arr->kv, sizeof(*arr->kv) * arr->capacity);
        if (!n) return 1;
        arr->kv = n;
    }
    if (arr->buffer_capacity < arr->buffer_curr_offset + keylen + datalen) {
        arr->buffer_capacity *= 2;
        void *n = realloc(arr->buffer, arr->buffer_capacity);
        if (!n) return 1;
        arr->buffer = n;
    }
    char *buffer = arr->buffer;
    void *keyloc = &buffer[arr->buffer_curr_offset];
    memcpy(keyloc, key, keylen);
    key_val_t *kv = &arr->kv[arr->items++];
    kv->key_start = arr->buffer_curr_offset;
    kv->key_len = keylen;
    arr->buffer_curr_offset += keylen;
    kv->data_len = datalen;

    if(datalen > 0) {
        void *dataloc = &buffer[arr->buffer_curr_offset];
        memcpy(dataloc, data, datalen);
        kv->data_start = arr->buffer_curr_offset;
        arr->buffer_curr_offset += datalen;
    }
    return 0;
}

int dyn_array_append(dyn_array_t *arr, void *key, int keylen, void *data, int datalen)
{
    if (arr->buffer_capacity > MAX_ARR_SZ && arr->bdb_env) {
        int rc = do_transfer(arr);
        if (rc) return rc;
    }
    if (arr->using_temp_table) {
        int bdberr;
        return bdb_temp_table_insert(arr->bdb_env, arr->temp_table_cur,
                key, keylen, data, datalen, &bdberr);
    }
    return append_to_array(arr, key, keylen, data, datalen);
}

void defered_index_array_dump(dyn_array_t *arr)
{
    for (int i = 0; i < arr->items; i++) {
        //logmsg(LOGMSG_ERROR, "AZ: %d: ", i); 
        char *buffer = arr->buffer;
        key_val_t *kv = &arr->kv[i];
        void *key = &buffer[kv->key_start];
        //hexdump(LOGMSG_ERROR, (const char *)key, kv->key_len);
        printf("%d: %d %d\n", i, *(int *)key, kv->key_len);
        //hexdump(LOGMSG_ERROR, "\n");
    }
}

int dyn_array_first(dyn_array_t *arr)
{
    if (arr->using_temp_table) {
        int err;
        return bdb_temp_table_first(arr->bdb_env, arr->temp_table_cur, &err);
    }
    if (arr->items < 1) return IX_EMPTY;
    arr->cursor = 0;
    return IX_OK;
}

int dyn_array_next(dyn_array_t *arr)
{
    if (arr->using_temp_table) {
        int err;
        return bdb_temp_table_next(arr->bdb_env, arr->temp_table_cur, &err);
    }
    if (++arr->cursor >= arr->items) 
        return IX_PASTEOF;
    return IX_OK;
}

void dyn_array_get_kv(dyn_array_t *arr, void **key, void **data, int *datalen)
{
    if (arr->using_temp_table) {
        *key = bdb_temp_table_key(arr->temp_table_cur);
        *data = bdb_temp_table_data(arr->temp_table_cur);
        *datalen = bdb_temp_table_datasize(arr->temp_table_cur);
        return;
    }
    if (arr->cursor >= arr->items) 
        abort();
    char *buffer = arr->buffer;
    key_val_t *tmp = &arr->kv[arr->cursor];
    *key = &buffer[tmp->key_start];
    *datalen = tmp->data_len;
    if(tmp->data_len > 0)
        *data = &buffer[tmp->data_start];
    else 
        *data = NULL;
}

int test_dyn_array()
{
    dyn_array_t arr = {0};
    for (int i = 10; i > 1; i--)
        dyn_array_append(&arr, &i, sizeof(i), NULL, 0);

    dyn_array_sort(&arr);
    defered_index_array_dump(&arr);
    abort();
}
