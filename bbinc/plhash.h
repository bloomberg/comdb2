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

#ifndef INCLUDED_PLHASH_H
#define INCLUDED_PLHASH_H

/* HASH  TABLE 10/08/01

   This is a generic hash table module.  It support generic key offset
   and length in a structure.  There are several wrappers for standard
   key types, like int*4 or c string.  Most common use is for fixed width
   byte fields.  This uses a shift/prime-mod algorithm that in practice
   produces nicely distributed hash values.  The hash table is also
   dynamically sized.  So it will adjust to the number of items in it.

   hash_add does NOT check for duplicate keys.  That's up to you to do.

   You need to lock around any hash routines for multi-threaded programs.

   Here's an example:

        struct my_record
        {
            char *name;
            int uuid;      // this is my key
        };

        struct my_record recs[3]=
        {
            { 1234, "John" },
            { 2345, "Jane" },
            { 3456, "Mary" }
        };

        hash_t *hash_table;
        struct my_record *rec;
        int uuid;

        // offsetof is in stddef.h - this hash table keys off of
        // uuid for size of int.
        hash_table=hash_init_o( offsetof(struct my_record, uuid), sizeof(int));

        // add my records
        hash_add(hash_table, &rec[0]);
        hash_add(hash_table, &rec[1]);
        hash_add(hash_table, &rec[2]);

        // now try to find some things.
        uuid=1;
        rec=(struct my_record*)hash_find(hash_table, &uuid);
                // rec would == 0, since uuid 1 is not in table.

        uuid=68812;
        rec=(struct my_record*)hash_find(hash_table, &uuid);
                // rec would == &rec[0], this is record 68812

        // now delete a record I found.
        hash_del(hash_table,rec);

  02/19/2003
 */

#include <stdio.h>
#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct hash hash_t;
typedef unsigned int hashfunc_t(const void *key, int len);
typedef int cmpfunc_t(const void *key1, const void *key2, int len);
typedef int hashforfunc_t(void *obj, void *arg);
typedef void *hashmalloc_t(size_t sz);
typedef void hashfree_t(void *ptr);

/* create hash table*/
hash_t *hash_init(int keylen);         /* fixed len keylen at offset 0 */
hash_t *hash_init_str(int keyoff);     /* string starts at keyoff */
hash_t *
hash_init_strcase(int keyoff); /* string starts at keyoff (case-insensitive) */
hash_t *hash_init_fnvstr(int keyoff);  /* string starts at keyoff; use FNV */
hash_t *hash_init_strptr(int keyoff);  /* string ptr at keyoff */
hash_t *hash_init_key0len(int keyoff); /* string with len in key[0] at keyoff */
hash_t *
hash_init_i4(int keyoff); /* i4 @ keyoff with some suitable optimisations */
hash_t *hash_init_o(int keyoff, int keylen); /* fixed len key at keyoff */
hash_t *hash_init_jenkins_o(
    int keyoff, int keylen); /* Bob Jenkins hash, power of 2 sized hash table */
hash_t *hash_init_user(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc, int keyoff,
                       int keyl);
hash_t *hash_setalloc_init(hashmalloc_t *hashmalloc, hashfree_t *hashfree,
                           int keyoff, int keysz);
hash_t *hash_setalloc_init_user(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc,
                                hashmalloc_t *hashmalloc, hashfree_t *hashfree,
                                int keyoff, int keysz);

/* hash_find() - find object given ptr to key
 * NOTE: this flips the object to the head of the chain so you must lock */
void *hash_find(hash_t *h, const void *key);

/* hash_findobj() - find object given ptr to an object */
void *hash_findobj(hash_t *h, const void *vobj);

/* hash_find_readonly() - find object given ptr to key; do not update hash table
 */
void *hash_find_readonly(hash_t *h, const void *key);

/* hash_findobj() - find object given ptr to an object; do not update hash table
 */
void *hash_findobj_readonly(hash_t *h, const void *vobj);

/* hash_add() - add object to hash table. 0 means success */
int hash_add(hash_t *h, void *obj);

/* hash_delk() - delete object from hash table by key. 0 means success */
int hash_delk(hash_t *h, const void *key);

/* hash_del() - delete object from hash table. 0 means success */
int hash_del(hash_t *h, const void *vobj);

/* hash_clear() - clear all items from hash table */
void hash_clear(hash_t *h);

/* free all resources associated with hash table (can't use after this!) */
void hash_free(hash_t *h);

/* free resized tables at synchronization point (for threaded programs) */
void hash_free_resized_tables(hash_t *h);

/* enable/disable stat collection for hash queries */
void hash_config_query_stats(hash_t *h, const int enable);

/* hash_dump - dump hash statistics to stdout, and detailed statistics
 * to out if out!=NULL */
void hash_dump(hash_t *h, FILE *out);

/* dump hash statistics to out and detailed usage stats to detail_out if
 * detail_out!=NULL. */
void hash_dump_stats(hash_t *h, FILE *out, FILE *detail_out);

/* hash_initsize - initializes the sz of a new (unused) hash */
int hash_initsize(hash_t *h, unsigned int sz);

/* hash_info - retrieve hash table info */
void hash_info(hash_t *h, int *nhits, int *nmisses, int *nsteps, int *ntbl,
               int *nents, int *nadds, int *ndels);
/* hash_info2 , same as hash_info plus max steps. */
void hash_info2(hash_t *h, int *nhits, int *nmisses, int *nsteps, int *ntbl,
                int *nents, int *nadds, int *ndels, int *maxsteps);

int hash_get_num_entries(hash_t *h);

/* hash_for - for each element,
 * call func() with ptr to object and user provided arg;
 * if func returns 0, then continue, else return */
int hash_for(hash_t *h, hashforfunc_t *func, void *arg);

/* if do not want to use hash_for and want to iterate through hash yourself
 * you can use hash_first and hash_next like so:
 * void *ent;
 * unsigned int bkt;
 * for (rec = (struct my_record*)hash_first(hash_table, &ent, &bkt);
 *          rec;
 *              rec = (struct my_record*)hash_next(hash_table, &ent, &bkt))
 * {
 *     do your thing for each rec
 *     ent and bkt hold hash table iterator state, don't change them
 * }
 */
void *hash_first(hash_t *h, void **ent, unsigned int *bkt);
void *hash_next(hash_t *h, void **ent, unsigned int *bkt);

/* The default hash function.  The default compare function is memcmp(). */
unsigned int hash_default_fixedwidth(const unsigned char *key, int len);

#ifdef __cplusplus
} /*extern "C"*/
#endif

#endif
