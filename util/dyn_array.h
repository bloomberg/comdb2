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


#include <stdbool.h>

typedef struct bdb_state_tag bdb_state_type;

/* The design for our key value dynamic array relies on two arrays: 
 *
 * kv_info  -> [a b c ...            ]
 * buffer   -> [key_a data_a key_b data_b ..  ]
 *
 * the first stores meta-information about the key value pair,
 * second we keep a buffer with the actual content of key and data.
 * This layout allows for quick random access of elements: dyn_arr[x],
 * since in pracice keys and values can be of different sizes.
 * When we sort, we only reorder the kv_info portion so that the data part
 * remains in place avoiding moving large memory sections during the sort.
 * Using this type of layout is beneficial for larger keys and larger values.
 */

struct dyn_array_t;

typedef struct {
#if !(defined _GNU_SOURCE || defined __GNU__ || defined __linux__)
    struct dyn_array_t *arr;
#endif
    int key_len;
    int key_start;
    int data_len;
    int data_start;
} kv_info_t;


/* Note that the intention for a dyamic array is to store things consecutively.
 * If you want items to be sorted you call sort() on the dynamic array.
 * However if the array spills to a temp_table then it will be automatically
 * sorted (so call to sort it explicitly is a noop) but it will be slower since
 * inserts into the temp_table btrees are more expensive and also 
 * nexts on the temp_table cursor are much slower than nexts on the array.
 */
typedef struct dyn_array_t {
    kv_info_t *kv;
    void *buffer;
    //comparator function, if not set will use memcmp
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2);
    void *temp_table;        // temp table if we have spilled
    void *temp_table_cur;
    bdb_state_type *bdb_env; // needed to spill to temptables
    int items;               // actual number of items in dyn array
    int capacity;            // key array capacity
    int buffer_capacity;
    int buffer_curr_offset;  // also serves as size used
    int cursor;              // object itself maintains this cursor
    bool using_temp_table:1;
    bool is_initialized:1;
} dyn_array_t;


void dyn_array_set_cmpr(dyn_array_t *arr,
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2));
void dyn_array_init(dyn_array_t *arr, void *bdb_env);
int dyn_array_first(dyn_array_t *arr);
int dyn_array_next(dyn_array_t *arr);
void dyn_array_get_key(dyn_array_t *arr, void **key);
void dyn_array_get_kv(dyn_array_t *arr, void **key, void **data, int *datalen);
int dyn_array_append(dyn_array_t *arr, void *key, int keylen, void *data, int datalen);
int dyn_array_sort(dyn_array_t *arr);
void dyn_array_close(dyn_array_t *arr);
void dyn_array_dump(dyn_array_t *arr);

#endif
