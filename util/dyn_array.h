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


#ifndef INCLUDED_DYN_ARRAY_H
#define INCLUDED_DYN_ARRAY_H



#define MAX_ARR_SZ 1024*1024

/*
 kv       -> [a b c                ]
 buffer   -> [key_a data_a key_b data_b ..  ]
 */

typedef struct {
    int key_len;
    int key_start;
    int data_len;
    int data_start;
} key_val_t;

typedef struct {
    key_val_t *kv;
    void *buffer;
    int capacity;
    int items;
    int buffer_capacity;
    int buffer_curr_offset; //also serves as size used
    int cursor;
    //comparator function, if not set will use memcmp
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2);
} dyn_array_t;




void dyn_array_set_cmpr(dyn_array_t *arr,
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2));
void dyn_array_init(dyn_array_t *arr);
int dyn_array_first(dyn_array_t *arr);
int dyn_array_next(dyn_array_t *arr);
void dyn_array_get_kv(dyn_array_t *arr, void **key, void **data, int *datalen);
int dyn_array_append(dyn_array_t *arr, void *key, int keylen, void *data, int datalen);
int dyn_array_sort(dyn_array_t *arr);
void dyn_array_close(dyn_array_t *arr);

#endif
