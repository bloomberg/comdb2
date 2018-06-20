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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <inttypes.h>
#include <sys/types.h>

/* DISABLE 'restrict' keyword usage pending further testing by Systems Group */
#define restrict

#include "pool.h"
#include "plhash.h"
#include "sysutil_membar.h"
#include "compile_time_assert.h"
#ifndef BUILDING_TOOLS
#include "mem_util.h"
#include "mem_override.h"
#endif
#include "logmsg.h"

typedef void *hash_kfnd_t(hash_t *const h, const void *const restrict vkey);

enum hash_scheme { HASH_BY_PRIMES, HASH_BY_POWER2 };

typedef struct hashent {
    struct hashent *next;
    unsigned int hash;
    unsigned char *obj;
} hashent;

typedef struct hashtable {
    struct hashtable *freed; /* to list of stale tables */
    unsigned int ntbl;       /* num entries in this table */
    hashent *tbl[];
} hashtable;
/* Point empty hash tables here. */
static struct {
    hashtable *freed;  /* to list of stale tables */
    unsigned int ntbl; /* num entries in this table */
    hashent *tbl[1];
} starter_htab = {NULL, 1, {NULL}};
#define STARTER_HTAB (struct hashtable *)&starter_htab
/*(allows skip of edge condition check for tbl == NULL, e.g. in hash lookups)*/
/* C99 supports flexible array members, but static initialization not portable.
 * Therefore, define structure replacing flexible array member with 1-element
 * array, and cast to 'struct hashent' when this structure is used.
 * Add compile-time assert as a safeguard to keep these structs in-sync
 */
BB_COMPILE_TIME_ASSERT(starter_htab_size,
                       sizeof(starter_htab) >=
                           sizeof(hashtable) + sizeof(hashent *));

struct hash {
    hashfunc_t *hashfunc;
    cmpfunc_t *cmpfunc;
    unsigned int keysz;
    int keyoff;
    hash_kfnd_t *hash_kfnd_fn_readonly;
    hash_kfnd_t *hash_kfnd_fn;
    pool_t *restrict ents;
    hashent *delayed; /* to list of stale hashents */
    unsigned int nents;
    hashtable *htab;
    int is_lockfree_query;
    unsigned int nsteps;
    unsigned int maxsteps;
    unsigned int nhits;
    unsigned int nmisses;
    unsigned int nadds;
    unsigned int ndels;
    unsigned int nlost;
    unsigned int ngrow;
    hashmalloc_t *malloc_fn;
    hashfree_t *free_fn;
    enum hash_scheme scheme;
};

enum { PRIME = 8388013 };

#define HASH(h, key) ((h)->hashfunc(key, (h)->keysz))
#define CMP(h, a, b) ((h)->cmpfunc(a, b, (h)->keysz))
#define BUCKET(hash, ntbl) ((hash) % (ntbl))

/* The hash find functions.  Since finding is a common operation I want these
 * to be very fast.  There are several versions with very minor changes to
 * suit different types of hash table. */

#ifndef __attribute_unused__
#ifdef __GNUC__
#define __attribute_unused__ __attribute__((unused))
#else
#define __attribute_unused__
#endif
#endif

/*
 * default and specialized hash query routines
 * hash the lookup value, calculate the bucket in which to look, and then
 * loop through the bucket chain using an appropriate comparison function.
 * The code is common to all functions after the loop condition,
 * so code in macro to avoid cut-n-paste duplication of the block.
 */

/* Must protect with mutex in threaded code */
#define HASH_QUERY_LOOP(h, tbl, he, condition)                                 \
    do {                                                                       \
        hashent **phe = 0; /*(assignment to avoid uninitialized warning)*/     \
        unsigned int nsteps = 0;                                               \
                                                                               \
        while (condition) {                                                    \
            phe = &he->next;                                                   \
            he = *phe;                                                         \
            ++nsteps;                                                          \
        }                                                                      \
        if (h->maxsteps < nsteps)                                              \
            h->maxsteps = nsteps;                                              \
        h->nsteps += nsteps;                                                   \
                                                                               \
        if (he) {                                                              \
            if (he != *tbl) {/* FLIP TO TOP OF LIST */                         \
                *phe = he->next;                                               \
                he->next = *tbl;                                               \
                *tbl = he;                                                     \
            }                                                                  \
            h->nhits++;                                                        \
            return he->obj;                                                    \
        } else {                                                               \
            h->nmisses++;                                                      \
            return 0;                                                          \
        }                                                                      \
                                                                               \
    } while (0)

/* thread-safe */
#define HASH_QUERY_LOOP_READONLY(h, he, condition)                             \
    do {                                                                       \
        unsigned int nsteps = 0;                                               \
                                                                               \
        while (condition) {                                                    \
            he = he->next;                                                     \
            ++nsteps;                                                          \
        }                                                                      \
        if (h->maxsteps < nsteps)                                              \
            h->maxsteps = nsteps;                                              \
        h->nsteps += nsteps;                                                   \
                                                                               \
        if (he) {                                                              \
            h->nhits++;                                                        \
            return he->obj;                                                    \
        } else {                                                               \
            h->nmisses++;                                                      \
            return 0;                                                          \
        }                                                                      \
                                                                               \
    } while (0)

/* Must protect with mutex in threaded code */
static void *default_hash_kfnd(hash_t *const h, const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = HASH(h, vkey);          /* hash */
    const unsigned int ii = BUCKET(hh, htab->ntbl); /* hash % prime */
    hashent **const restrict tbl = htab->tbl + ii;  /* chain head */
    hashent *he = *tbl;
    const int keyoff = h->keyoff; /* key offset */

#if 0 /* (macro is expanded inline below for clarity) */
    HASH_QUERY_LOOP(h,tbl,he,                              /* match condition */
		    (he && !(he->hash==hh && CMP(h,vkey,&he->obj[keyoff])==0)));
#endif

    hashent **phe = 0; /*(assignment to avoid uninitialized warning)*/
    unsigned int nsteps = 0;

    while (he && !(he->hash == hh && CMP(h, vkey, &he->obj[keyoff]) == 0)) {
        phe = &he->next;
        he = *phe;
        ++nsteps;
    }
    if (h->maxsteps < nsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
        if (he != *tbl) { /* FLIP TO TOP OF LIST */
            *phe = he->next;
            he->next = *tbl;
            *tbl = he;
        }
        h->nhits++;
        return he->obj;
    } else {
        h->nmisses++;
        return 0;
    }
}

/* Must protect with mutex in threaded code */
static void *power2_hash_kfnd(hash_t *const h, const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = HASH(h, vkey);         /* hash */
    const unsigned int ii = hh & (htab->ntbl - 1); /* hash % power2 */
    hashent **const restrict tbl = htab->tbl + ii; /* chain head */
    hashent *he = *tbl;
    const int keyoff = h->keyoff; /* key offset */

#if 0 /* (macro is expanded inline below for clarity) */
    HASH_QUERY_LOOP(h,tbl,he,                              /* match condition */
		    (he && !(he->hash==hh && CMP(h,vkey,&he->obj[keyoff])==0)));
#endif

    hashent **phe = 0; /*(assignment to avoid uninitialized warning)*/
    unsigned int nsteps = 0;

    while (he && !(he->hash == hh && CMP(h, vkey, &he->obj[keyoff]) == 0)) {
        phe = &he->next;
        he = *phe;
        ++nsteps;
    }
    if (h->maxsteps < nsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
        if (he != *tbl) { /* FLIP TO TOP OF LIST */
            *phe = he->next;
            he->next = *tbl;
            *tbl = he;
        }
        h->nhits++;
        return he->obj;
    } else {
        h->nmisses++;
        return 0;
    }
}

/* Must protect with mutex in threaded code */
static void *i4_hash_kfnd(hash_t *const h, const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = *((const unsigned int *)vkey); /* hash */
    const unsigned int ii = BUCKET(hh, htab->ntbl);        /* hash % prime */
    hashent **const restrict tbl = htab->tbl + ii;         /* chain head */
    hashent *he = *tbl;

#if 0 /* (macro is expanded inline below for clarity) */
    HASH_QUERY_LOOP(h,tbl,he,(he && he->hash != hh));      /* match condition */
#endif

    hashent **phe = 0; /*(assignment to avoid uninitialized warning)*/
    unsigned int nsteps = 0;

    while (he && he->hash != hh) {
        phe = &he->next;
        he = *phe;
        ++nsteps;
    }
    if (h->maxsteps < nsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
        if (he != *tbl) { /* FLIP TO TOP OF LIST */
            *phe = he->next;
            he->next = *tbl;
            *tbl = he;
        }
        h->nhits++;
        return he->obj;
    } else {
        h->nmisses++;
        return 0;
    }
}

/* thread-safe */
static void *default_hash_kfnd_readonly(hash_t *const h,
                                        const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = HASH(h, vkey);          /* hash */
    const unsigned int ii = BUCKET(hh, htab->ntbl); /* hash % prime */
    register hashent *restrict he = htab->tbl[ii];  /* hashent chain */
    const int keyoff = h->keyoff;                   /* key offset */

#if 0 /* (macro is expanded inline below for clarity) */
    HASH_QUERY_LOOP_READONLY(h,he,                         /* match condition */
	(he && !(he->hash==hh && CMP(h,vkey,&he->obj[keyoff])==0)));
#endif

    unsigned int nsteps = 0;

    while (he && !(he->hash == hh && CMP(h, vkey, &he->obj[keyoff]) == 0)) {
        he = he->next;
        ++nsteps;
    }
    if (h->maxsteps < nsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
        h->nhits++;
        return he->obj;
    } else {
        h->nmisses++;
        return 0;
    }
}

/* thread-safe */
static void *power2_hash_kfnd_readonly(hash_t *const h,
                                       const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = HASH(h, vkey);         /* hash */
    const unsigned int ii = hh & (htab->ntbl - 1); /* hash % power2 */
    register hashent *restrict he = htab->tbl[ii]; /* hashent chain */
    const int keyoff = h->keyoff;                  /* key offset */

#if 0 /* (macro is expanded inline below for clarity) */
    HASH_QUERY_LOOP_READONLY(h,he,                         /* match condition */
	(he && !(he->hash==hh && CMP(h,vkey,&he->obj[keyoff])==0)));
#endif

    unsigned int nsteps = 0;

    while (he && !(he->hash == hh && CMP(h, vkey, &he->obj[keyoff]) == 0)) {
        he = he->next;
        ++nsteps;
    }
    if (h->maxsteps < nsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
        h->nhits++;
        return he->obj;
    } else {
        h->nmisses++;
        return 0;
    }
}

/* thread-safe */
static void *i4_hash_kfnd_readonly(hash_t *const h,
                                   const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = *((const unsigned int *)vkey); /* hash */
    const unsigned int ii = BUCKET(hh, htab->ntbl);        /* hash % prime */
    register hashent *restrict he = htab->tbl[ii];         /* hashent chain */

#if 0 /* (macro is expanded inline below for clarity) */
    HASH_QUERY_LOOP_READONLY(h,he,(he && he->hash != hh)); /* match condition */
#endif

    unsigned int nsteps = 0;

    while (he && he->hash != hh) {
        he = he->next;
        ++nsteps;
    }
    if (h->maxsteps < nsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
        h->nhits++;
        return he->obj;
    } else {
        h->nmisses++;
        return 0;
    }
}

/* thread-safe */
static void *default_hash_kfnd_nofrills(hash_t *const h,
                                        const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = HASH(h, vkey);          /* hash */
    const unsigned int ii = BUCKET(hh, htab->ntbl); /* hash % prime */
    register hashent *restrict he = htab->tbl[ii];  /* hashent chain */
    const int keyoff = h->keyoff;                   /* key offset */
                                                    /* match condition */
    while (he && !(he->hash == hh && CMP(h, vkey, &he->obj[keyoff]) == 0))
        he = he->next;

    return he ? he->obj : 0;
}

/* thread-safe */
static void *power2_hash_kfnd_nofrills(hash_t *const h,
                                       const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = HASH(h, vkey);         /* hash */
    const unsigned int ii = hh & (htab->ntbl - 1); /* hash % power2 */
    register hashent *restrict he = htab->tbl[ii]; /* hashent chain */
    const int keyoff = h->keyoff;                  /* key offset */
                                                   /* match condition */
    while (he && !(he->hash == hh && CMP(h, vkey, &he->obj[keyoff]) == 0))
        he = he->next;

    return he ? he->obj : 0;
}

/* thread-safe */
static void *i4_hash_kfnd_nofrills(hash_t *const h,
                                   const void *const restrict vkey)
{
    hashtable *const htab = h->htab;
    const unsigned int hh = *((const unsigned int *)vkey); /* hash */
    const unsigned int ii = BUCKET(hh, htab->ntbl);        /* hash % prime */
    register hashent *restrict he = htab->tbl[ii];         /* hashent * chain */

    while (he && he->hash != hh) /* match condition */
        he = he->next;

    return he ? he->obj : 0;
}

#if 0 /* Template for the hash_fnd functions. */


/*
 * Template for the hash_fnd functions.
 *
 * Before including this define the following macros:
 * 	HASH_KFND_NAME - name of function
 *  HASH_KFND_POWER2 - define this if this is for a power of 2 sized hash
 *  				   table (avoids expensive modulo)
 *  HASH_KFND_CMP_I4 - set this to do the comparison directly as a four byte
 *                     compare rather than invoking the compare function
 *  HASH_KFND_HASH_I4 - set this to bypass the hash function and use the
 *                      key as a 4 byte hash value
 *
 * These will all be undefined ready for the next inclusion
 *
 * Must protect with mutex in threaded code.
 */

static void *HASH_KFND_NAME(hash_t * const h,
			    const void * const restrict vkey)
{
    hashtable * const htab = h->htab;

#ifdef HASH_KFND_HASH_I4
    const unsigned int hh = *((const unsigned int*)vkey);
#else
    const unsigned int hh = HASH(h,vkey);
#endif

#ifdef HASH_KFND_POWER2
    const unsigned int ii = hh & (htab->ntbl - 1);
#else
    const unsigned int ii = BUCKET(hh, htab->ntbl);
#endif

#if defined(HASH_KFND_CMP_I4) && !defined(HASH_KFND_HASH_I4)
    const int ikey = *(const int *)vkey;
#endif

#if !(defined(HASH_KFND_CMP_I4) && defined(HASH_KFND_HASH_I4))
    const int keyoff = h->keyoff;
#endif

    int nsteps = 0;
    hashent ** const restrict tbl = htab->tbl + ii;/* point to chain head */
    hashent *he = *tbl;
    hashent **phe;

#ifdef HASH_KFND_CMP_I4
#ifdef HASH_KFND_HASH_I4
    while (he && he->hash != hh)
#else
    while (he && *((const int *)&he->obj[keyoff]) != ikey)
#endif
#else
    while (he && !(he->hash == hh && CMP(h,vkey,&he->obj[keyoff]) == 0))
#endif
    {
	phe = &he->next;
	he  = *phe;
	++nsteps;
    }
    if (h->maxsteps < nsteps)
	h->maxsteps = nsteps;
    h->nsteps += nsteps;

    if (he) {
	if(he != *tbl) { /* FLIP TO TOP OF LIST */
	    *phe     = he->next;
	    he->next = *tbl;
	    *tbl     = he;
	}
	h->nhits++;
	return he->obj;
    }
    else {
	h->nmisses++;
	return 0;
    }
}

#define double_to_expand(_name) _name##_nofrills
#define nostats_fn(name) double_to_expand(name)
static void *nostats_fn(HASH_KFND_NAME)(hash_t * const h,
					const void * const restrict vkey)
{
    hashtable * const htab = h->htab;

#ifdef HASH_KFND_HASH_I4
    const unsigned int hh = *((const unsigned int*)vkey);
#else
    const unsigned int hh = HASH(h,vkey);
#endif

#ifdef HASH_KFND_POWER2
    const unsigned int ii = hh & (htab->ntbl - 1);
#else
    const unsigned int ii = BUCKET(hh, htab->ntbl);
#endif

    register hashent * restrict he = htab->tbl[ii];        /* hashent chain */

#if defined(HASH_KFND_CMP_I4) && !defined(HASH_KFND_HASH_I4)
    const int ikey = *(const int *)vkey;
#endif

#if !(defined(HASH_KFND_CMP_I4) && defined(HASH_KFND_HASH_I4))
    const int keyoff = h->keyoff;
#endif

#ifdef HASH_KFND_CMP_I4
#ifdef HASH_KFND_HASH_I4
    while (he && he->hash != hh)
#else
    while (he && *((const int *)&he->obj[keyoff]) != ikey)
#endif
#else
    while (he && !(he->hash == hh && CMP(h,vkey,&he->obj[keyoff]) == 0))
#endif
    {
	he = he->next;
    }

    return he ? he->obj : 0;
}
#undef double_to_expand
#undef nostats_fn

#ifdef HASH_KFND_CMP_I4
#undef HASH_KFND_CMP_I4
#endif

#ifdef HASH_KFND_HASH_I4
#undef HASH_KFND_HASH_I4
#endif

#ifdef HASH_KFND_POWER2
#undef HASH_KFND_POWER2
#endif

#undef HASH_KFND_NAME

#endif /* Template for the hash_fnd functions. */

/*
-------------------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.

This is reversible, so any information in (a,b,c) before mix() is
still in (a,b,c) after mix().

If four pairs of (a,b,c) inputs are run through mix(), or through
mix() in reverse, there are at least 32 bits of the output that
are sometimes the same for one pair and different for another pair.
This was tested for:
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
satisfy this are
    4  6  8 16 19  4
    9 15  3 18 27 15
   14  9  3  7 17  3
Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
for "differ" defined as + with a one-bit base and a two-bit delta.  I
used http://burtleburtle.net/bob/hash/avalanche.html to choose
the operations, constants, and arrangements of the variables.

This does not achieve avalanche.  There are input bits of (a,b,c)
that fail to affect some output bits of (a,b,c), especially of a.  The
most thoroughly mixed value is c, but it doesn't really even achieve
avalanche in c.

This allows some parallelism.  Read-after-writes are good at doubling
the number of bits affected, so the goal of mixing pulls in the opposite
direction as the goal of parallelism.  I did what I could.  Rotates
seem to cost as much as shifts on every machine I could lay my hands
on, and rotates are much kinder to the top and bottom bits, so I used
rotates.
-------------------------------------------------------------------------------
*/

#define rot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))

#define mix(a, b, c)                                                           \
    {                                                                          \
        a -= c;                                                                \
        a ^= rot(c, 4);                                                        \
        c += b;                                                                \
        b -= a;                                                                \
        b ^= rot(a, 6);                                                        \
        a += c;                                                                \
        c -= b;                                                                \
        c ^= rot(b, 8);                                                        \
        b += a;                                                                \
        a -= c;                                                                \
        a ^= rot(c, 16);                                                       \
        c += b;                                                                \
        b -= a;                                                                \
        b ^= rot(a, 19);                                                       \
        a += c;                                                                \
        c -= b;                                                                \
        c ^= rot(b, 4);                                                        \
        b += a;                                                                \
    }

/*
-------------------------------------------------------------------------------
final -- final mixing of 3 32-bit values (a,b,c) into c

Pairs of (a,b,c) values differing in only a few bits will usually
produce values of c that look totally different.  This was tested for
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

These constants passed:
 14 11 25 16 4 14 24
 12 14 25 16 4 14 24
and these came close:
  4  8 15 26 3 22 24
 10  8 15 26 3 22 24
 11  8 15 26 3 22 24
-------------------------------------------------------------------------------
*/
#define final(a, b, c)                                                         \
    {                                                                          \
        c ^= b;                                                                \
        c -= rot(b, 14);                                                       \
        a ^= c;                                                                \
        a -= rot(c, 11);                                                       \
        b ^= a;                                                                \
        b -= rot(a, 25);                                                       \
        c ^= b;                                                                \
        c -= rot(b, 16);                                                       \
        a ^= c;                                                                \
        a -= rot(c, 4);                                                        \
        b ^= a;                                                                \
        b -= rot(a, 14);                                                       \
        c ^= b;                                                                \
        c -= rot(b, 24);                                                       \
    }

/*
 * hashbig():
 * This is the same as hashword() on big-endian machines.  It is different
 * from hashlittle() on all machines.  hashbig() takes advantage of
 * big-endian byte ordering.
 */
/*
 * From http://www.burtleburtle.net/bob/hash/doobs.html
 * This function is public domain code.
 * Modified by SJ to make this fit in this module - length is now int not size_t
 * (to match the other hash functions, this will matter in 64 bit) and
 * initval is gone, presumed zero)
 * The test program below is faster if it uses this rather than the regular
 * default hash by about 1.5x.  Maybe we could be even faster if we were to
 * compile this optimised.
 */
static uint32_t jenkins_hashbig(const void *key, int length)
{
    static const uint32_t initval = 0;
    uint32_t a, b, c;
    union {
        const void *ptr;
        size_t i;
    } u; /* to cast key to (size_t) happily */

    /* Set up the internal state */
    a = b = c = 0xdeadbeef + ((uint32_t)length) + initval;

    u.ptr = key;
    if (/*HASH_BIG_ENDIAN && */ ((u.i & 0x3) == 0)) {
        const uint32_t *k = (const uint32_t *)key; /* read 32-bit chunks */

        /*------ all but last block: aligned reads and affect 32 bits of (a,b,c)
         */
        while (length > 12) {
            a += k[0];
            b += k[1];
            c += k[2];
            mix(a, b, c);
            length -= 12;
            k += 3;
        }

/*----------------------------- handle the last (probably partial) block */
/*
 * "k[2]<<8" actually reads beyond the end of the string, but
 * then shifts out the part it's not allowed to read.  Because the
 * string is aligned, the illegal read is in the same word as the
 * rest of the string.  Every machine with memory protection I've seen
 * does it on word boundaries, so is OK with this.  But VALGRIND will
 * still catch it and complain.  The masking trick does make the hash
 * noticably faster for short strings (like English words).
 */
#ifndef VALGRIND

        switch (length) {
        case 12:
            c += k[2];
            b += k[1];
            a += k[0];
            break;
        case 11:
            c += k[2] & 0xffffff00;
            b += k[1];
            a += k[0];
            break;
        case 10:
            c += k[2] & 0xffff0000;
            b += k[1];
            a += k[0];
            break;
        case 9:
            c += k[2] & 0xff000000;
            b += k[1];
            a += k[0];
            break;
        case 8:
            b += k[1];
            a += k[0];
            break;
        case 7:
            b += k[1] & 0xffffff00;
            a += k[0];
            break;
        case 6:
            b += k[1] & 0xffff0000;
            a += k[0];
            break;
        case 5:
            b += k[1] & 0xff000000;
            a += k[0];
            break;
        case 4:
            a += k[0];
            break;
        case 3:
            a += k[0] & 0xffffff00;
            break;
        case 2:
            a += k[0] & 0xffff0000;
            break;
        case 1:
            a += k[0] & 0xff000000;
            break;
        case 0:
            return c; /* zero length strings require no mixing */
        }

#else /* make valgrind happy */

        const uint8_t *k8 = (const uint8_t *)k;
        switch (length) /* all the case statements fall through */
        {
        case 12:
            c += k[2];
            b += k[1];
            a += k[0];
            break;
        case 11:
            c += ((uint32_t)k8[10]) << 8; /* fall through */
        case 10:
            c += ((uint32_t)k8[9]) << 16; /* fall through */
        case 9:
            c += ((uint32_t)k8[8]) << 24; /* fall through */
        case 8:
            b += k[1];
            a += k[0];
            break;
        case 7:
            b += ((uint32_t)k8[6]) << 8; /* fall through */
        case 6:
            b += ((uint32_t)k8[5]) << 16; /* fall through */
        case 5:
            b += ((uint32_t)k8[4]) << 24; /* fall through */
        case 4:
            a += k[0];
            break;
        case 3:
            a += ((uint32_t)k8[2]) << 8; /* fall through */
        case 2:
            a += ((uint32_t)k8[1]) << 16; /* fall through */
        case 1:
            a += ((uint32_t)k8[0]) << 24;
            break;
        case 0:
            return c;
        }

#endif /* !VALGRIND */

    } else { /* need to read the key one byte at a time */
        const uint8_t *k = (const uint8_t *)key;

        /*--------------- all but the last block: affect some 32 bits of (a,b,c)
         */
        while (length > 12) {
            a += ((uint32_t)k[0]) << 24;
            a += ((uint32_t)k[1]) << 16;
            a += ((uint32_t)k[2]) << 8;
            a += ((uint32_t)k[3]);
            b += ((uint32_t)k[4]) << 24;
            b += ((uint32_t)k[5]) << 16;
            b += ((uint32_t)k[6]) << 8;
            b += ((uint32_t)k[7]);
            c += ((uint32_t)k[8]) << 24;
            c += ((uint32_t)k[9]) << 16;
            c += ((uint32_t)k[10]) << 8;
            c += ((uint32_t)k[11]);
            mix(a, b, c);
            length -= 12;
            k += 12;
        }

        /*-------------------------------- last block: affect all 32 bits of (c)
         */
        switch (length) /* all the case statements fall through */
        {
        case 12:
            c += k[11];
        case 11:
            c += ((uint32_t)k[10]) << 8;
        case 10:
            c += ((uint32_t)k[9]) << 16;
        case 9:
            c += ((uint32_t)k[8]) << 24;
        case 8:
            b += k[7];
        case 7:
            b += ((uint32_t)k[6]) << 8;
        case 6:
            b += ((uint32_t)k[5]) << 16;
        case 5:
            b += ((uint32_t)k[4]) << 24;
        case 4:
            a += k[3];
        case 3:
            a += ((uint32_t)k[2]) << 8;
        case 2:
            a += ((uint32_t)k[1]) << 16;
        case 1:
            a += ((uint32_t)k[0]) << 24;
            break;
        case 0:
            return c;
        }
    }

    final(a, b, c);
    return c;
}

static int hash_default_strcmp(const void *a, const void *b,
                               int len __attribute_unused__)
{
    return strcmp(a, b);
}

/* case-insensitive */
static int hash_default_strcasecmp(const void *a, const void *b,
                                   int len __attribute_unused__)
{
    return strcasecmp(a, b);
}

static unsigned int hash_default_strlen(const unsigned char *key,
                                        int len __attribute_unused__)
{
    unsigned hash;
    for (hash = 0; *key; key++)
        hash = (((hash % PRIME) << 8) + (*key));
    return hash;
}

/* This is the public domain FNV-1 hash function from
 * http://isthe.com/chongo/tech/comp/fnv/ */
#define FNV_OFFSET_BASIS 2166136261U
#define FNV_PRIME 16777619
static unsigned int hash_fnv_strlen(const unsigned char *key,
                                    int len __attribute_unused__)
{
    unsigned hash;
    for (hash = FNV_OFFSET_BASIS; *key; key++) {
        hash = (hash * FNV_PRIME) ^ *key;
    }
    return hash;
}

static int hash_default_strptrcmp(const char **a, const char **b,
                                  int len __attribute_unused__)
{
    return strcmp(*a, *b);
}

static unsigned int hash_default_strptrlen(const unsigned char **keyp,
                                           int len __attribute_unused__)
{
    unsigned hash;
    const unsigned char *key = *keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % PRIME) << 8) + (*key);
    return hash;
}

static int hash_default_key0cmp(const void *a, const void *b, int len)
{
    len = ((const unsigned char *)a)[0] + 1; /*LEN INCLUDING LENGTH*/
    return memcmp(a, b, len);
}

static unsigned int hash_default_key0len(const unsigned char *key, int len)
{
    unsigned hash;
    int jj;
    len = key[0]; /*neg len means 1st byte has len*/
    for (hash = 0, jj = 0; jj < len; jj++)
        hash = ((hash % PRIME) << 8) + key[jj];
    return hash;
}

unsigned int hash_default_fixedwidth(const unsigned char *key, int len)
{
    unsigned hash;
    int jj;
    for (hash = 0, jj = 0; jj < len; jj++)
        hash = ((hash % PRIME) << 8) + key[jj];
    return hash;
}

static int hash_default_i4cmp(const int *a, const int *b,
                              int len __attribute_unused__)
{
    return (*a) - (*b);
}

static unsigned int hash_default_i4(const unsigned int *a,
                                    int len __attribute_unused__)
{
    return a[0];
}

/* enable lock-free hash query (no need for mutex in threaded applications)
 * Caller promises to use only hash_findobj_readonly() and hash_find_readonly()
 * for hash queries and acknowledges that both hash buckets and hash table will
 * be copied in full upon table resize, and not free()d until hash is destroyed.
 * (Cost: memory usage for hash buckets and hash table will almost double)
 * At application synchronization points, hash_free_resized_tables() can be
 * called by the application to release deleted/resized internal hash structures
 */
void hash_config_lockfree_query(hash_t *const h) { h->is_lockfree_query = 1; }

/* enable stats for query steps and flipping found entry to head of chain.
 * or, disable stats and set query function to *_nofrills, which skips stats
 * and is always readonly (required when configuring lockfree query) */
void hash_config_query_stats(hash_t *const h, const int enable)
{
    if (enable) {
        if (h->hash_kfnd_fn == default_hash_kfnd_nofrills) {
            h->hash_kfnd_fn = default_hash_kfnd;
            h->hash_kfnd_fn_readonly = default_hash_kfnd_readonly;
        } else if (h->hash_kfnd_fn == power2_hash_kfnd_nofrills) {
            h->hash_kfnd_fn = power2_hash_kfnd;
            h->hash_kfnd_fn_readonly = power2_hash_kfnd_readonly;
        } else if (h->hash_kfnd_fn == i4_hash_kfnd_nofrills) {
            h->hash_kfnd_fn = i4_hash_kfnd;
            h->hash_kfnd_fn_readonly = i4_hash_kfnd_readonly;
        }
    } else {
        if (h->hash_kfnd_fn == default_hash_kfnd) {
            h->hash_kfnd_fn = default_hash_kfnd_nofrills;
            h->hash_kfnd_fn_readonly = default_hash_kfnd_nofrills;
        } else if (h->hash_kfnd_fn == power2_hash_kfnd) {
            h->hash_kfnd_fn = power2_hash_kfnd_nofrills;
            h->hash_kfnd_fn_readonly = power2_hash_kfnd_nofrills;
        } else if (h->hash_kfnd_fn == i4_hash_kfnd) {
            h->hash_kfnd_fn = i4_hash_kfnd_nofrills;
            h->hash_kfnd_fn_readonly = i4_hash_kfnd_nofrills;
        }
    }
}

static hash_t *hash_init_int(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc,
                             hashmalloc_t *hashmalloc, hashfree_t *hashfree,
                             int keyoff, int keysz, hash_kfnd_t *hash_kfnd,
                             enum hash_scheme scheme)
{
    hash_t *h;
    pool_t *p;
    if (keyoff < 0)
        return 0;
    if (keysz < 0)
        return 0;
    p = pool_setalloc_init(sizeof(hashent), 32, hashmalloc, hashfree);
    if (p == 0)
        return 0;
    h = (hash_t *)hashmalloc(sizeof(hash_t));
    if (h == 0) {
        pool_free(p);
        return 0;
    }
    memset(h, 0, sizeof(hash_t));
    h->ents = p;
    h->htab = STARTER_HTAB; /*(allows skip of edge condition for tbl==NULL)*/
    h->keysz = keysz;
    h->keyoff = keyoff;
    h->hashfunc = hashfunc;
    h->cmpfunc = cmpfunc;
    h->malloc_fn = hashmalloc;
    h->free_fn = hashfree;
    if (hash_kfnd == default_hash_kfnd)
        h->hash_kfnd_fn_readonly = default_hash_kfnd_readonly;
    else if (hash_kfnd == power2_hash_kfnd)
        h->hash_kfnd_fn_readonly = power2_hash_kfnd_readonly;
    else if (hash_kfnd == i4_hash_kfnd)
        h->hash_kfnd_fn_readonly = i4_hash_kfnd_readonly;
    else
        h->hash_kfnd_fn_readonly = hash_kfnd;
    h->hash_kfnd_fn = hash_kfnd;
    h->scheme = scheme;
    return h;
}

hash_t *hash_setalloc_init_user(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc,
                                hashmalloc_t *hashmalloc, hashfree_t *hashfree,
                                int keyoff, int keysz)
{
    return hash_init_int(hashfunc, cmpfunc, hashmalloc, hashfree, keyoff, keysz,
                         default_hash_kfnd, HASH_BY_PRIMES);
}

hash_t *hash_setalloc_init(hashmalloc_t *hashmalloc, hashfree_t *hashfree,
                           int keyoff, int keysz)
{
    return hash_setalloc_init_user((hashfunc_t *)hash_default_fixedwidth,
                                   (cmpfunc_t *)memcmp, hashmalloc, hashfree,
                                   keyoff, keysz);
}

hash_t *hash_init_user(hashfunc_t *hashfunc, cmpfunc_t *cmpfunc, int keyoff,
                       int keysz)
{
    return hash_setalloc_init_user(hashfunc, cmpfunc, malloc, free, keyoff,
                                   keysz);
}

hash_t *hash_init_strptr(int keyoff)
{
    return hash_init_user((hashfunc_t *)hash_default_strptrlen,
                          (cmpfunc_t *)hash_default_strptrcmp, keyoff, 0);
}

hash_t *hash_init_str(int keyoff)
{
    return hash_init_user((hashfunc_t *)hash_default_strlen,
                          (cmpfunc_t *)hash_default_strcmp, keyoff, 0);
}

/* case-insensitive */
hash_t *hash_init_strcase(int keyoff)
{
    return hash_init_user((hashfunc_t *)hash_default_strlen,
                          (cmpfunc_t *)hash_default_strcasecmp, keyoff, 0);
}

hash_t *hash_init_fnvstr(int keyoff)
{
    return hash_init_int((hashfunc_t *)hash_fnv_strlen,
                         (cmpfunc_t *)hash_default_strcmp, malloc, free, keyoff,
                         0, power2_hash_kfnd, HASH_BY_POWER2);
}

hash_t *hash_init_key0len(int keyoff)
{
    return hash_init_user((hashfunc_t *)hash_default_key0len,
                          (cmpfunc_t *)hash_default_key0cmp, keyoff, 0);
}

/* I experimented with a Bob Jenkins hash for i4.  This performed worse than
 * this version because the advantage of not doing a modulo was lost to the
 * disadvantage of having to invoke the hash function through a function
 * pointer. */
hash_t *hash_init_i4(int keyoff)
{
    return hash_init_int((hashfunc_t *)hash_default_i4,
                         (cmpfunc_t *)hash_default_i4cmp, malloc, free, keyoff,
                         4, i4_hash_kfnd, HASH_BY_PRIMES);
}

hash_t *hash_init_o(int keyoff, int keylen)
{
    return hash_init_user((hashfunc_t *)hash_default_fixedwidth,
                          (cmpfunc_t *)memcmp, keyoff, keylen);
}

hash_t *hash_init_jenkins_o(int keyoff, int keylen)
{
    return hash_init_int((hashfunc_t *)jenkins_hashbig, (cmpfunc_t *)memcmp,
                         malloc, free, keyoff, keylen, power2_hash_kfnd,
                         HASH_BY_POWER2);
}

hash_t *hash_init(int keylen)
{
    return hash_init_user((hashfunc_t *)hash_default_fixedwidth,
                          (cmpfunc_t *)memcmp, 0, keylen);
}

int hash_initsize(hash_t *h, unsigned int sz)
{
    const size_t tsz = sizeof(hashtable) + sz * sizeof(hashent *);

    if (sz == 0 || h->htab != STARTER_HTAB)
        return -1;

    if ((h->htab = h->malloc_fn(tsz)) != 0) {
        memset(h->htab, 0, tsz);
        h->htab->ntbl = sz;
        if (h->scheme == HASH_BY_POWER2 && (sz & (sz - 1)))
            logmsg(LOGMSG_ERROR, "%s: bad size %u for power of 2 hash table\n",
                    __func__, sz);
        return 0;
    } else {
        h->htab = STARTER_HTAB;
        return -1;
    }
}

void *hash_findobj(hash_t *const restrict h, const void *const restrict vobj)
{
    return h->hash_kfnd_fn(h, (((const unsigned char *)vobj) + h->keyoff));
}

void *hash_find(hash_t *const restrict h, const void *const restrict key)
{
    return h->hash_kfnd_fn(h, key);
}

void *hash_findobj_readonly(hash_t *const restrict h,
                            const void *const restrict vobj)
{
    return h->hash_kfnd_fn_readonly(h,
                                    ((const unsigned char *)vobj) + h->keyoff);
}

void *hash_find_readonly(hash_t *const restrict h,
                         const void *const restrict key)
{
    return h->hash_kfnd_fn_readonly(h, key);
}

/*
 * hash_inctbl() resizes hash table (hash_inctbl() called only from hash_add())
 */

static void hash_inctbl_rehash_primes(hashtable *const newhtab,
                                      hashtable *const htab)
{
    hashent *he, *nhe;
    unsigned int ii, jj;
    hashent **const restrict tbl = htab->tbl;
    hashent **const restrict newtbl = newhtab->tbl;
    const unsigned int ntbl = htab->ntbl;
    const unsigned int newntbl = newhtab->ntbl;

    for (ii = 0; ii < ntbl; ++ii) {
        for (he = tbl[ii]; he; he = nhe) {
            jj = BUCKET(he->hash, newntbl);
            nhe = he->next;
            he->next = newtbl[jj];
            newtbl[jj] = he;
        }
    }
}

static void hash_inctbl_rehash_power2(hashtable *const newhtab,
                                      hashtable *const htab)
{
    hashent *he, *nhe;
    unsigned int ii, jj;
    hashent **const restrict tbl = htab->tbl;
    hashent **const restrict newtbl = newhtab->tbl;
    const unsigned int ntbl = htab->ntbl;
    const unsigned int newntbl = newhtab->ntbl - 1; /*used by power2 hash*/

    for (ii = 0; ii < ntbl; ++ii) {
        for (he = tbl[ii]; he; he = nhe) {
            jj = he->hash & newntbl;
            nhe = he->next;
            he->next = newtbl[jj];
            newtbl[jj] = he;
        }
    }
}

static void hash_inctbl_rehash_copy_primes(hashtable *const newhtab,
                                           hashtable *const htab,
                                           hashent *restrict he_chain)
{
    /* he_chain is preallocated and *must* contain h->nents num of hashent */

    hashent *he, *nhe;
    unsigned int ii, jj;
    hashent **const restrict tbl = htab->tbl;
    hashent **const restrict newtbl = newhtab->tbl;
    const unsigned int ntbl = htab->ntbl;
    const unsigned int newntbl = newhtab->ntbl;

    for (ii = 0; ii < ntbl; ++ii) {
        for (he = tbl[ii]; he; he = he->next) {
            nhe = he_chain;
            he_chain = nhe->next;
            nhe->obj = he->obj;
            nhe->hash = he->hash;
            jj = BUCKET(he->hash, newntbl);
            nhe->next = newtbl[jj];
            newtbl[jj] = nhe;
        }
    }
}

static void hash_inctbl_rehash_copy_power2(hashtable *const newhtab,
                                           hashtable *const htab,
                                           hashent *restrict he_chain)
{
    /* he_chain is preallocated and *must* contain h->nents num of hashent */

    hashent *he, *nhe;
    unsigned int ii, jj;
    hashent **const restrict tbl = htab->tbl;
    hashent **const restrict newtbl = newhtab->tbl;
    const unsigned int ntbl = htab->ntbl;
    const unsigned int newntbl = newhtab->ntbl - 1; /*used by power2 hash*/

    for (ii = 0; ii < ntbl; ++ii) {
        for (he = tbl[ii]; he; he = he->next) {
            nhe = he_chain;
            he_chain = nhe->next;
            nhe->obj = he->obj;
            nhe->hash = he->hash;
            jj = he->hash & newntbl;
            nhe->next = newtbl[jj];
            newtbl[jj] = nhe;
        }
    }
}

static hashent *hash_inctbl_getablk_bulk(hash_t *const h)
{
    /* <<<TODO: add block allocate for pool_getablk of blocks */
    /* (would prefer to allocate h->nents all at once; oh well) */

    size_t nents = (size_t)h->nents; /*must not be 0; caller checks return val*/
    hashent ent;
    hashent *he = &ent;
    pool_t *const restrict ents = h->ents;

    do {
        he->next = (hashent *)pool_getablk(ents);
    } while ((he = he->next) && --nents);

    if (he) {
        he->next = NULL;
        return ent.next;
    }

    /* error; handle out of memory condition */
    while ((he = ent.next)) {
        ent.next = he->next;
        pool_relablk(ents, he);
    }
    return NULL;
}

/* Called only from hash_add().
 * Returns 0 on failure; otherwise the new table length.
 * If there is a failure to grow the hash table, but the prior (incoming) table
 * is -not- starter_htab, then the prior hash table is returned, which is still
 * functional, though performance may degrade as it becomes more crowded.
 */
static hashtable *hash_inctbl(hash_t *const h)
{
    static const unsigned int prime[] = {257,    1031,    4099,    16411,
                                         32771,  65537,   131101,  262147,
                                         524309, 1048583, 4194319, 0};

    hashtable *restrict newhtab;
    hashtable *const restrict htab = h->htab;
    const unsigned int ntbl = htab->ntbl;
    unsigned int newntbl, tsz;

    if (h->scheme == HASH_BY_PRIMES) {
        unsigned int ii;
        for (ii = 0; prime[ii] <= ntbl && prime[ii] != 0; ++ii)
            ;
        newntbl = (prime[ii] != 0) ? prime[ii] : h->nents * 4 - 1;
    } else { /* HASH_BY_POWER2 */
        newntbl = (ntbl != 0) ? (ntbl << 1) : 256;
    }
    tsz = sizeof(hashtable) + newntbl * sizeof(hashent *);
    if ((newhtab = h->malloc_fn(tsz)) == 0)
        return htab; /* previous hashtable */

    memset(newhtab, 0, tsz);
    newhtab->freed = htab;
    newhtab->ntbl = newntbl;

    /* insert (or lock-free copy) existing entries into new hash table
     * (separate loop for each case to avoid extra conditionals inside loop)
     * (about to walk every bucket in a potentially large table) */
    if (h->nents) {
        if (!h->is_lockfree_query) {
            if (h->scheme == HASH_BY_PRIMES) /*HASH_BY_PRIMES*/
                hash_inctbl_rehash_primes(newhtab, htab);
            else /*HASH_BY_POWER2*/
                hash_inctbl_rehash_power2(newhtab, htab);
        } else {
            /* copy buckets to avoid interference with lock-free queries */

            hashent *const restrict he_chain = hash_inctbl_getablk_bulk(h);
            if (he_chain == NULL) {
                h->free_fn(newhtab);
                return htab; /* previous hashtable */
            }

            if (h->scheme == HASH_BY_PRIMES) /*HASH_BY_PRIMES*/
                hash_inctbl_rehash_copy_primes(newhtab, htab, he_chain);
            else /*HASH_BY_POWER2*/
                hash_inctbl_rehash_copy_power2(newhtab, htab, he_chain);
        }
    }

    SYSUTIL_MEMBAR_RELEASE();
    h->htab = newhtab; /* assignment of new table is atomic; thread-safe */

    /* If lockfree_query is set, skip free() for thread safety.
     * Another thread might be in the middle of using the hash for
     * lock-free query.  Instead, it's on a list for freeing later.
     */
    if (htab == STARTER_HTAB)
        newhtab->freed = 0;
    else if (!h->is_lockfree_query) {
        newhtab->freed = 0;
        h->free_fn(htab);
    }
    h->ngrow++;
    return newhtab;
}

int hash_add(hash_t *h, void *vobj)
{
    /* must be protected by mutex in threaded application */
    unsigned char *const restrict obj = (unsigned char *)vobj;
    hashtable *restrict htab = h->htab;
    hashent *restrict he;
    hashent **tbl;
    if (h->nents >= htab->ntbl >> 1) {
        if ((htab = hash_inctbl(h)) == STARTER_HTAB)
            return -1; /*(failed to resize starter_htab)*/
    }
    he = (hashent *)pool_getablk(h->ents);
    if (he == 0)
        return -1;
    he->obj = obj;
    he->hash = HASH(h, &obj[h->keyoff]);
    tbl = &htab->tbl[BUCKET(he->hash, htab->ntbl)];
    he->next = *tbl;
    SYSUTIL_MEMBAR_RELEASE();
    *tbl = he;
    h->nadds++;
    h->nents++;
    return 0;
}

int hash_delk(hash_t *const h, const void *const key)
{
    /* must be protected by mutex in threaded application */
    hashtable *const restrict htab = h->htab;
    unsigned int nsteps = 0;
    const unsigned int hh = HASH(h, key);
    hashent **const chead = htab->tbl + BUCKET(hh, htab->ntbl);
    hashent **phe = chead;
    hashent *he = *chead;
    hashent *restrict fhe;
    while (he && !(he->hash == hh && CMP(h, key, &he->obj[h->keyoff]) == 0)) {
        phe = &he->next;
        he = he->next;
        nsteps++;
    }
    if (nsteps > h->maxsteps)
        h->maxsteps = nsteps;
    h->nsteps += nsteps;
    if (he == 0)
        return -1;
    if (!h->is_lockfree_query) {
        /* SNIP & RETURN */
        (*phe) = he->next;
        pool_relablk(h->ents, he);
    } else {
        /* If bucket to be deleted is not at head of chain,
         * carefully move to head of chain by creating loop
         * in the chain, changing head, and breaking loop.
         * Memory barriers must be used between each step,
         * or else there can be a race in multiple deletes
         * removing two consecutive nodes which can corrupt
         * the chain. */
        if (*chead != he) {
            /* (he reused; (*phe) points to he to delete) */
            while (he->next)
                he = he->next; /*find chain end*/
            he->next = *chead; /*create loop*/
            SYSUTIL_MEMBAR_RELEASE();
            he = *chead = *phe; /*change head*/
            SYSUTIL_MEMBAR_RELEASE();
            *phe = NULL; /*break loop*/
        }

        /* Simply swap off the chain head (assignment is atomic)
         * Do not reuse deleted buckets to avoid collisions
         * with lock-free queries that might be reading bucket.
         * Bucket will not be reused until hash is cleared.
         */
        *chead = he->next;
        /* Try to keep it pointed to by a list */
        if ((fhe = pool_getablk(h->ents)) != 0) {
            fhe->obj = (unsigned char *)he;
            fhe->next = h->delayed;
            h->delayed = fhe;
        } else {
            h->nlost++;
        }
    }
    h->ndels++;
    h->nents--;
    return 0;
}

int hash_del(hash_t *const h, const void *const vobj)
{
    return hash_delk(h, ((const unsigned char *)vobj) + h->keyoff);
}

void hash_clear(hash_t *const h)
{
    hashfree_t *const h_free = h->free_fn;
    hashtable *nxtab, *restrict htab = h->htab;
    if (htab != STARTER_HTAB) {
        /* pool_clear() below will reclaim hashents) */
        memset(htab->tbl, 0, htab->ntbl * sizeof(hashent *));
        nxtab = htab->freed;
        htab->freed = 0;
        while ((htab = nxtab)) {
            nxtab = htab->freed;
            h_free(htab);
        }
    }
    h->delayed = 0;
    h->nents = 0;
    pool_clear(h->ents);
}

void hash_free(hash_t *const h)
{
    hashfree_t *const h_free = h->free_fn;
    hashtable *restrict htab, *nxtab = h->htab->freed;
    while ((htab = nxtab)) {
        nxtab = htab->freed;
        h_free(htab);
    }
    if (h->htab != STARTER_HTAB)
        h_free(h->htab);
    pool_free(h->ents);
    memset(h, -1, sizeof(*h)); /* zap it */
    h_free(h);
}

/* (for threaded programs to free resized tables at synchronization point) */
/* (Alternate implementation could allocate new h->ent for each table and
 *  call pool_clear()/pool_free() instead of pool_relablk on every bucket.
 *  Then, 'delayed' would also be per htab) */
void hash_free_resized_tables(hash_t *const h)
{
    hashfree_t *const h_free = h->free_fn;
    pool_t *const restrict ents = h->ents;
    hashtable *nxtab, *restrict htab = h->htab;
    hashent *he, *nhe, **tbl;
    size_t i, sz;

    /* First clear out any lingering stale hashents we own */
    /* 'delayed' is a list of hashents whose objects are deleted hashents */
    for (he = h->delayed; he != 0; he = nhe) {
        nhe = he->next;
        pool_relablk(ents, he->obj); /* stale hashent */
        pool_relablk(ents, he);
    }
    h->delayed = 0;

    nxtab = htab->freed;
    htab->freed = 0;
    while ((htab = nxtab)) {
        nxtab = htab->freed;
        tbl = htab->tbl;
        sz = htab->ntbl;
        for (i = 0; i < sz; ++i) {
            nhe = tbl[i];
            while ((he = nhe)) {
                nhe = he->next;
                pool_relablk(ents, he);
            }
        }
        h_free(htab);
    }
}

void hash_dump(hash_t *h, FILE *out) { hash_dump_stats(h, stdout, out); }

void hash_dump_stats(hash_t *h, FILE *out, FILE *detail_out)
{
    unsigned int ii, jj;
    unsigned int cnts[16];
    hashtable *const restrict htab = h->htab;
    hashent **const tbl = htab->tbl;
    const unsigned int ntbl = htab->ntbl;
    int nused;
    char buf[160];
    hashent *he;
    pool_info(h->ents, 0, &nused, 0);
    logmsgf(LOGMSG_USER, out, "Key Size = %-10u      #Ents = %-10u\n", h->keysz, h->nents);
    logmsgf(LOGMSG_USER, out, "#Table   = %-10u      #Used = %-10d\n", ntbl, nused);
    logmsgf(LOGMSG_USER, out, "#Steps   = %-10u   MaxSteps = %-10u\n", h->nsteps,
            h->maxsteps);
    logmsgf(LOGMSG_USER, out, "#Hits    = %-10u    #Misses = %-10u\n", h->nhits, h->nmisses);
    logmsgf(LOGMSG_USER, out, "#Adds    = %-10u      #Dels = %-10u\n", h->nadds, h->ndels);
    logmsgf(LOGMSG_USER, out, "#TBLgrow = %-10u      #Lost = %-10u\n", h->ngrow, h->nlost);
    bzero(cnts, sizeof(cnts));
    buf[0] = 0;
    for (ii = 0; ii < ntbl; ii++) {
        for (jj = 0, he = tbl[ii]; he && jj < 15; jj++, he = he->next)
            ;
        if (detail_out != 0) {
            snprintf(buf + (ii & 0x3f), sizeof(buf) - (ii & 0x3f), "%x", jj);
            if ((ii & 0x3f) == 0x3f)
                logmsgf(LOGMSG_USER, detail_out, "%s\n", buf);
        }
        cnts[jj]++;
    }
    if (detail_out != 0) {
        if ((ii & 0x3f) != 0x3f)
            logmsgf(LOGMSG_USER, detail_out, "%s\n", buf);
    }
    for (ii = 1; ii < 15; ii++)
        logmsgf(LOGMSG_USER, out, "# OF BUCKETS WITH %10d ENTRIES: %d\n", ii, cnts[ii]);
    logmsgf(LOGMSG_USER, out, "# OF BUCKETS WITH >=%8d ENTRIES: %d\n", ii, cnts[ii]);
}

int hash_for(hash_t *const h, hashforfunc_t *const func,
             void *const restrict arg)
{
    unsigned int ii;
    int rc;
    hashent *restrict he, *nhe;
    hashtable *const htab = h->htab;
    hashent **const tbl = htab->tbl;
    const unsigned int ntbl = htab->ntbl;

    for (ii = 0; ii < ntbl; ii++) {
        for (he = tbl[ii]; he; he = nhe) {
            nhe = he->next;
            rc = (*func)(he->obj, arg);
            if (rc != 0)
                return rc; /*terminate walk*/
        }
    }
    return 0;
}

void *hash_first(hash_t *const h, void **const ent,
                 unsigned int *const restrict bkt)
{
    hashent *he = 0;
    hashtable *const htab = h->htab;
    hashent **const restrict tbl = htab->tbl;
    const unsigned int ntbl = htab->ntbl;
    unsigned int ii;

    for (ii = 0; ii < ntbl && !(he = tbl[ii]); ++ii)
        ;
    *bkt = ii;
    if (he) {
        *ent = he->next;
        return he->obj;
    }
    return 0;
}

void *hash_next(hash_t *const h, void **const ent,
                unsigned int *const restrict bkt)
{
    hashent *restrict he = (hashent *)(*ent);
    if (!he) {
        hashtable *const htab = h->htab;
        hashent **const tbl = htab->tbl;
        const unsigned int ntbl = htab->ntbl;
        unsigned int ii;
        for (ii = *bkt + 1; ii < ntbl && !(he = tbl[ii]); ++ii)
            ;
        *bkt = ii;
        if (!he)
            return 0;
    }
    *ent = he->next;
    return he->obj;
}

void hash_info2(hash_t *h, int *nhits, int *nmisses, int *nsteps, int *ntbl,
                int *nents, int *nadds, int *ndels, int *maxsteps)
/*added maxsteps*/
{
    if (nhits)
        *nhits = h->nhits;
    if (nmisses)
        *nmisses = h->nmisses;
    if (nsteps)
        *nsteps = h->nsteps;
    if (ntbl)
        *ntbl = h->htab->ntbl;
    if (nents)
        *nents = h->nents;
    if (nadds)
        *nadds = h->nadds;
    if (ndels)
        *ndels = h->ndels;
    if (maxsteps)
        *maxsteps = h->maxsteps;
}

void hash_info(hash_t *h, int *nhits, int *nmisses, int *nsteps, int *ntbl,
               int *nents, int *nadds, int *ndels)
{
    hash_info2(h, nhits, nmisses, nsteps, ntbl, nents, nadds, ndels, 0);
}

int hash_get_num_entries(hash_t *h) { return h->nents; }

#ifdef HASH_TEST_PROGRAM

static void genkey_seed(int seed) { srand48(seed); }

static void genkey(char *key, int len)
{
    int ii;
    for (ii = 6; ii < len; ii++)
        key[ii] = (lrand48() % 75) + 48;
    key[0] = key[1] = key[2] = 0x20;
    key[3] = key[4] = key[5] = 0x20;
}

struct obj {
    int dat;
    char key[16];
} * objs, *op;

static int cnt(void *obj, void *arg)
{
    ((int *)arg)[0]++;
    return 0;
}

int main(int argc, char *argv[])
{
    enum { MAX = 5000000, ITER = 2 };
    int ii, jj, kk, rc;
    hash_t *h;
    objs = (struct obj *)malloc(MAX * ITER * sizeof(struct obj));
    if (objs == 0) {
        perror("cant alloc mem!");
        exit(1);
    }
    /*h=hash_init_o(4,16);*/
    h = hash_init_jenkins_o(4, 16);
    printf("ADDING TO HASH\n");
    for (kk = 1; kk <= ITER; kk++) {
        genkey_seed(kk);
        jj = 0;
        for (ii = 0; ii < MAX * kk; ii++) {
            genkey(&objs[jj].key[0], 16);
            objs[jj].dat = jj;
            op = hash_find(h, (unsigned char *)objs[jj].key);
            if (op == 0) {
                rc = hash_add(h, (unsigned char *)(objs + jj));
                jj++;
            } else {
                printf("DUP KEY ADDING %s\n", objs[jj].key);
            }
#ifdef VERBOSE
            if ((ii & 0xfff) == 0)
                printf("ADDING, ITERATION %d\n", ii);
#endif
        }
        printf("DONE. ADDED %d\n", jj);
        hash_dump(h, 0);
        for (ii = 0; ii < jj; ii++) {
            rc = hash_del(h, (unsigned char *)&objs[ii]);
            if (rc == -1) {
                printf("HASH FAILED DELETING %s\n", objs[jj].key);
            }
            if (objs[ii].dat != ii) {
                printf("DATA MISMATCH! %08x %08x\n", objs[jj].dat, ii);
            }
#ifdef VERBOSE
            if ((ii & 0xfff) == 0)
                printf("DEL'D %d\n", ii);
#endif
        }
    } /*1-5*/
    printf("DONE!\n");

    jj = 0;
    for (ii = 0; ii < MAX; ii++) {
        genkey(&objs[jj].key[0], 16);
        objs[jj].dat = jj;
        op = hash_find(h, (unsigned char *)objs[jj].key);
        if (op == 0) {
            rc = hash_add(h, (unsigned char *)(objs + jj));
            jj++;
        } else {
            printf("DUP KEY ADDING %s\n", objs[jj].key);
        }
#ifdef VERBOSE
        if ((ii & 0xfff) == 0)
            printf("ADDING, ITERATION %d\n", ii);
#endif
    }
    ii = 0;
    hash_for(h, cnt, &ii);
    printf("COUNTED %d ITEMS\n", ii);
    printf("CLEAR\n");
    hash_dump(h, 0);
    hash_clear(h);
    hash_dump(h, 0);
    ii = 0;
    hash_for(h, cnt, &ii);
    printf("COUNTED %d ITEMS\n", ii);
    printf("FREE\n");
    hash_free(h);
    return 0;
}
#endif
