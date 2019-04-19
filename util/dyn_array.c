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


#include "dyn_array.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>

/* compatible with comdb2 */
enum IXRCODES {
    IX_OK = 0 /*OPERATION SUCCESSFUL*/
    ,
    IX_FND = 0 /*KEY FOUND*/
    ,
    IX_FNDMORE = 1 /*KEY FOUND, ANOTHER MATCH AHEAD*/
    ,
    IX_NOTFND = 2 /*KEY NOT FOUND*/
    ,
    IX_PASTEOF = 3 /*KEY NOT FOUND, HIGHER THAN ALL KEYS*/
    ,
    IX_DUP = 2 /*DUP ON ADD*/
    ,
    IX_FNDNOCONV = 12 /*FOUND BUT COULDN'T CONVERT*/
    ,
    IX_FNDMORENOCONV = 13 /*FOUND, ANOTHER MATCH AHEAD, BUT COULDN'T CONVERT*/
    ,
    IX_NOTFNDNOCONV = 14 /*DIDN'T FIND, COULDN'T CONVERT NEXT*/
    ,
    IX_PASTEOFNOCONV = 15 /*HIGHER THAN ALL KEYS, COULDN'T CONVERT LAST*/
    ,
    IX_SCHEMACHANGED = 16 /*SCHEMA CHANGED SINCE LAST FIND*/
    ,
    IX_ACCESS = 17 /*CAN'T ACCESS TABLE */
    ,
    IX_EMPTY = 99 /*DB EMPTY*/
};

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
    if (arr->kv) {
        assert(arr->capacity > 0);
        free(arr->kv);
    }
    if (arr->buffer) {
        assert(arr->buffer_capacity > 0);
        free(arr->buffer);
    }
    memset(arr, 0, sizeof(dyn_array_t));
}

void dyn_array_init(dyn_array_t *arr)
{
    if (arr->capacity > 0)
        dyn_array_close(arr);
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

int dyn_array_append(dyn_array_t *arr, void *key, int keylen, void *data, int datalen)
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

void defered_index_array_dump(dyn_array_t *arr)
{
    for (int i = 0; i < arr->items; i++) {
        //logmsg(LOGMSG_ERROR, "AZ: %d: ", i); 
        char *buffer = arr->buffer;
        void *key = &buffer[arr->kv[i].key_start];
        //hexdump(LOGMSG_ERROR, (const char *)key, arr->kv[i].key_len);
        printf("%d: %d %d\n", i, *(int *)key, arr->kv[i].key_len);
        //hexdump(LOGMSG_ERROR, "\n");
    }
}

int dyn_array_first(dyn_array_t *arr)
{
    if (arr->items < 1) return IX_EMPTY;
    arr->cursor = 0;
    return IX_OK;
}

int dyn_array_next(dyn_array_t *arr)
{
    if (++arr->cursor >= arr->items) 
        return IX_PASTEOF;
    return IX_OK;
}

void dyn_array_get_kv(dyn_array_t *arr, void **key, void **data, int *datalen)
{
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
