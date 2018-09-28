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

#ifndef __genid_h__
#define __genid_h__

typedef uint64_t genid_t;

/* genid's bytes don't get reordered on little-endian machines, this means we
 * need different bitmasks on those architectures */
#ifdef _LINUX_SOURCE
/* little endian */
#define GENID_STRIPE_MASK (0x0f00000000000000ULL)
#define GENID_STRIPE_SHIFT (56)
/*                                 USUUDDDDTTTTTTTT */
#define GENID_NORMALIZE_MASK (0x0f00ffffffffffffULL)
#define GENID_SLOT_MASK (0xf0ff000000000000ULL)
#define GENID_SLOT_PRE_SHIFT (48)
#define GENID_SLOT_POST_SHIFT (4)

/* synthetic genid types */
/*
    synthetic-add == (0x80)
    synthetic-upd == (0x80 | 0x40)
*/
#define GENID_SYNTHETIC_MASK (0x00000000000000C0ULL)
#define GENID_SYNTHETIC_ADD (0x0000000000000080ULL)
#define GENID_SYNTHETIC_BIT (0x0000000000000080ULL)
#define GENID_SYNTHETIC_UPD (0x00000000000000C0ULL)

#else

/* big endian */
#define GENID_STRIPE_MASK (0x000000000000000fULL)
#define GENID_STRIPE_SHIFT (0)
/*                                 TTTTTTTTDDDDUUUS */
#define GENID_NORMALIZE_MASK (0xffffffffffff000fULL)
#define GENID_SLOT_MASK (0x000000000000fff0ULL)
#define GENID_SLOT_PRE_SHIFT (0)
#define GENID_SLOT_POST_SHIFT (4)

/* synthetic genid types */
/*
    synthetic-add == (0x80)
    synthetic-upd == (0x80 | 0x40)
*/
#define GENID_SYNTHETIC_MASK (0xC000000000000000ULL)
#define GENID_SYNTHETIC_BIT (0x8000000000000000ULL)
#define GENID_SYNTHETIC_ADD (0x8000000000000000ULL)
#define GENID_SYNTHETIC_UPD (0xC000000000000000ULL)

#endif

#define GENID_OLD_DUP_MASK (0xffffffff0000000fULL)

static inline int is_genid_add(unsigned long long crt)
{
    return (crt & GENID_SYNTHETIC_MASK) == GENID_SYNTHETIC_ADD;
}

static inline int is_genid_upd(unsigned long long crt)
{
    return (crt & GENID_SYNTHETIC_MASK) == GENID_SYNTHETIC_UPD;
}

static inline int is_genid_synthetic(unsigned long long crt)
{
    return ((crt & GENID_SYNTHETIC_BIT) != 0);
}

static inline void set_genid_add(unsigned long long *crt)
{
    *crt |= GENID_SYNTHETIC_ADD;
}

static inline void set_genid_upd(unsigned long long *crt)
{
    *crt |= GENID_SYNTHETIC_UPD;
}

/* the synthetic genids will be "visible" only for sql interface */
static inline int get_dtafile_from_genid(unsigned long long genid)
{
    unsigned int dtafile;

    /* is a synthetic genid ? */
    if (is_genid_add(genid))
        return -1;
    if (is_genid_upd(genid))
        return -2;

    dtafile = (genid & GENID_STRIPE_MASK) >> GENID_STRIPE_SHIFT;

    return dtafile;
}

struct bdb_state_type;
unsigned long long bdb_get_next_genid(bdb_state_type *bdb_state);

#endif
