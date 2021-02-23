/*
   Copyright 2021, Bloomberg Finance L.P.

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

#ifndef INCLUDE_GLUE_H
#define INCLUDE_GLUE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <inttypes.h>

typedef uint32_t       db_pgno_t;      /* Page number type. */
typedef uint16_t       db_indx_t;      /* Page offset type. */
typedef uint32_t       db_recno_t;     /* Record number type. */
typedef uint64_t db_alignp_t;

#define DB_RENAMEMAGIC  0x030800        /* File has been renamed. */
#define DB_BTREEMAGIC   0x053162
#define DB_HASHMAGIC    0x061561
#define DB_QAMMAGIC     0x042253

#define DB_AM_CHKSUM            0x00000001 /* Checksumming. */
#define DB_AM_ENCRYPT           0x00000800 /* Encryption. */
#define DB_AM_SWAP              0x10000000 /* Pages need to be byte-swapped. */
#define DB_FILE_ID_LEN          20      /* Unique file ID length. */
#define DB_IV_BYTES     16              /* Bytes per IV */
#define DB_MAC_KEY      20              /* Bytes per MAC checksum */

#define SSZ(name, field)  P_TO_UINT16(&(((name *)0)->field))
#define SSZA(name, field) P_TO_UINT16(&(((name *)0)->field[0]))
#define P_TO_UINT16(p)  ((uint16_t)(db_alignp_t)(p))

typedef enum {
        DB_BTREE=1,
        DB_HASH=2,
        DB_RECNO=3,
        DB_QUEUE=4,
        DB_UNKNOWN=5                   /* Figure it out on open. */
} DBTYPE;

typedef struct __db_lsn {
        uint32_t       file;           /* File ID. */
        uint32_t       offset;         /* File offset. */
} DB_LSN;

typedef struct __db {
        uint32_t pgsize;               /* Database logical page size. */
	char *fname;
	int flags;
#define DB_PFX_COMP           0x0000001
#define DB_SFX_COMP           0x0000002
#define DB_RLE_COMP           0x0000004
	uint8_t compression_flags;
	int offset_bias;
} DB;

/* Set, clear and test flags. */
#define FLD_CLR(fld, f)         (fld) &= ~(f)
#define FLD_ISSET(fld, f)       ((fld) & (f))
#define FLD_SET(fld, f)         (fld) |= (f)
#define F_CLR(p, f)             (p)->flags &= ~(f)
#define F_ISSET(p, f)           ((p)->flags & (f))
#define F_SET(p, f)             (p)->flags |= (f)
#define LF_CLR(f)               ((flags) &= ~(f))
#define LF_ISSET(f)             ((flags) & (f))
#define LF_SET(f)               ((flags) |= (f))

#include "ar_page.h"

int fileid(const char *, int, DBMETA *);

#ifdef __cplusplus
}
#endif
#endif
