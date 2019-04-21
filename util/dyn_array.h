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

#define MAX_ARR_SZ 1024*24024
typedef struct bdb_state_tag bdb_state_type;

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


/* Note that the intention for a dyamic array is to store things consecutively
 * If you want items to be sorted you call sort() on the dynamic array.
 * However if the array spills to a temp_table then it will be automatically
 * sorted. This may be good because you don't need to sort it explicitly, 
 * but it may be bad because you will loose the original order of insertion.
 */
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
    void *temp_table;
    void *temp_table_cur;
    bdb_state_type *bdb_env; // needed to spill to temptables
    bool using_temp_table;
} dyn_array_t;




void dyn_array_set_cmpr(dyn_array_t *arr,
    int (*compar)(void *usermem, int key1len,
                                 const void *key1, int key2len,
                                 const void *key2));
void dyn_array_init(dyn_array_t *arr, void *bdb_env);
int dyn_array_first(dyn_array_t *arr);
int dyn_array_next(dyn_array_t *arr);
void dyn_array_get_kv(dyn_array_t *arr, void **key, void **data, int *datalen);
int dyn_array_append(dyn_array_t *arr, void *key, int keylen, void *data, int datalen);
int dyn_array_sort(dyn_array_t *arr);
void dyn_array_close(dyn_array_t *arr);

#endif
