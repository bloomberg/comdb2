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

#ifndef INCLUDED_MARSHAL
#define INCLUDED_MARSHAL

/*RESOURCE MARSHALLER

  These functions provide a convenient way to free up data at clean up
  time for complex data structures.   Many times I am processing input
  and building a complicate list of data structs.  On error, I want to
  free up the data.  If I use a 'marshal' struct to associate my allocated
  data, I can simply call release_marshal().  This frees up in reverse
  order all data.  Standard malloc is wrapped up.  Non-standard allocators
  can be added, as long as their free routine matches marshal_free_func_t
  prototype.  You can allocate with custom allocator and add free routine
  with 'marshal_add'.

  An example of a non-standard resource:  hash routines in /bbinc/pl/hash.h

       hash_t *hash_table;
       marshal_t *marshaller;

       // create a marshaller
       marshaller=new_marshal("my data struct");

       // create a hash table
       hash_table=hash_init_str(0);

       // now add hash table to marshaller
       marshal_add(marsher, hash_table, (marshal_free_func_t*)hash_free);

       // I'll add some more resources here...
       marshal_strdup("this will get automatically freed");
       marshal_strdup("this too");

       // Now to automatically free up, in reverse order:
       release_marshal(marshaler);

  2/19/2003

  original version
  09/25/2002

*/
#include <stdio.h>

typedef struct marshal_t marshal_t;
typedef void marshal_free_func_t(void *);

#ifdef __cplusplus
extern "C" {
#endif

/* new resource marshaller*/
marshal_t *new_marshal(char *name);

/*free up marshaller structure.  DOES NOT FREE ANY RESOURCES in marshaller
  the end result would be as if allocations were done without marshaller.  */
void free_marshal_only(marshal_t *mm);

/* free all resources and free marshaller */

/* this frees in reverse order of add.  last in, first freed*/
void *release_marshal(marshal_t *mm);
/* this flavor calls release_marshal and prints error, with \n.  returns
 * NULL...*/
void *release_marshal_perr(marshal_t *mm, char *format, ...);
/* this flavor calls release_marshal and prints msg, with no \n.  returns
 * NULL...*/
void *release_marshal_pmsg(marshal_t *mm, char *format, ...);
/* this flavor calls release_marshal and returns specified int rcode ...*/
int release_marshal_rc(marshal_t *mm, int rcode);

/*marshal a resource*/
void *marshal_add(marshal_t *mm, void *ptr, marshal_free_func_t *free_func);

/*delete individual item from marshaller. 0=success*/
int marshal_del(marshal_t *mm, void *ptr);

/*deletes original item, replaces with new item-FREES ORIGINAL ITEM, keeps
 * position*/
void *marshal_replace(marshal_t *mm, void *orig, void *new);
/*change pointer-DOES NOT FREE ORIGINAL ITEM, keeps position in marshaller*/
void *marshal_chg(marshal_t *mm, void *orig, void *new);

/*malloc a buffer and add to marshal buffer*/
void *marshal_malloc(marshal_t *mm, size_t size);

/*calloc a buffer and add to marshal buffer*/
void *marshal_calloc(marshal_t *mm, size_t nelem, size_t elsiz);

/*reallocate a malloc'd buffer, maintain priority in marshaller*/
void *marshal_realloc(marshal_t *mm, void *orig, size_t size);

/*marshal a string allocated with malloc/strdup/etc.*/
char *marshal_add_str(marshal_t *mm, char *str);
char *marshal_strdup(marshal_t *mm, char *str);
char *marshal_tokdup(marshal_t *mm, char *str, int lenstr);

/* marshal a child marshaller */
marshal_t *marshal_add_marshal(marshal_t *mm, marshal_t *child);

/* utility rtn to dump marshal contents */
void dump_marshal(FILE *out, marshal_t *mm);

#ifdef __cplusplus
} /*extern "C"*/
#endif

#endif
