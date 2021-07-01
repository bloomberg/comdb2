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

#include <stdint.h>
#include "db_wrap.h"
#include "db_checksum.h"

uint32_t myflip(uint32_t in)
{
    union intswp
    {
        uint32_t u32;
        uint8_t u8[4];
    };
    typedef union intswp intswp_t;
    intswp_t in_val, flip_val;
    in_val.u32 = in;
    flip_val.u8[0] = in_val.u8[3];
    flip_val.u8[1] = in_val.u8[2];
    flip_val.u8[2] = in_val.u8[1];
    flip_val.u8[3] = in_val.u8[0];
    return flip_val.u32;
}

dbfile_info *dbfile_init(const char *filename)
{
    // This comes straight from pgdump - can't ask berkeley to do this
    // because db might be encrypted and we don't have passwd
    uint8_t meta_buf[DBMETASIZE];
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        return NULL;
    }

    dbfile_info *file = malloc(sizeof(dbfile_info));
    if (file == NULL) {
        goto err;
    }

    size_t n = read(fd, meta_buf, DBMETASIZE);
    if (n == -1) {
        goto err;
    }

    close(fd);

    DBMETA *meta = (DBMETA *)meta_buf;

    uint8_t swapped = 0;
    uint32_t flags = meta->flags;
    uint32_t magic = meta->magic;
    uint32_t pagesize = meta->pagesize;

again:
    switch (magic) {
    case DB_BTREEMAGIC:
        if (LF_ISSET(BTM_RECNO))
            file->type = DB_RECNO;
        else
            file->type = DB_BTREE;
        break;
    case DB_HASHMAGIC:
        file->type = DB_HASH;
        break;
    case DB_QAMMAGIC:
        file->type = DB_QUEUE;
        break;
    default:
        if (swapped) {
            goto err;
        }
        swapped = 1;
        magic = myflip(magic);
        flags = myflip(flags);
        pagesize = myflip(pagesize);
        goto again;
    }
    file->pagesize = pagesize;
    file->metaflags = meta->metaflags;
    file->is_encrypted = meta->encrypt_alg;
    file->is_swapped = swapped;
    return file;

err:
    close(fd);
    free(file);
    return NULL;
}

void dbfile_deinit(dbfile_info *f) {
    free(f);
}

const char *dbfile_filename(dbfile_info *f)
{
    return f->filename;
}

size_t dbfile_pagesize(dbfile_info *f)
{
    return f->pagesize;
}

uint32_t dbfile_is_encrypted(dbfile_info *f)
{
    return f->is_encrypted;
}

uint32_t dbfile_is_swapped(dbfile_info *f)
{
    return f->is_swapped;
}

uint32_t dbfile_is_checksummed(dbfile_info *f)
{
    return (f->metaflags & DBMETA_CHKSUM) ? 1 : 0;
}

uint32_t dbfile_is_sparse(dbfile_info *f)
{
    int rc;
    struct stat statbuf;

    rc = stat(f->filename, &statbuf);
    if (rc != 0) {
        return 0;
    }

    if (statbuf.st_size > (statbuf.st_blocks * statbuf.st_blksize))
        return 1;

    return 1;
}

// Verify the checksum on a regular Berkeley DB page. Returns true if the
// checksum is correct, false otherwise.
int verify_checksum(uint8_t *page, size_t pagesize, uint32_t is_encrypted,
                    uint32_t is_swapped, uint32_t *verify_cksum)
{
    PAGE *pagep = (PAGE *)page;
    uint8_t *chksum_ptr = page;

    switch (PTYPE(pagep)) {
    case P_HASHMETA:
    case P_BTREEMETA:
    case P_QAMMETA:
        chksum_ptr = ((BTMETA *)page)->chksum;
        pagesize = DBMETASIZE;
        break;
    default:
        chksum_ptr += is_encrypted ? SIZEOF_PAGE + SSZA(PG_CRYPTO, chksum)
            : SIZEOF_PAGE + SSZA(PG_CHKSUM, chksum);
        break;
    }

    uint32_t orig_chksum, chksum;
    orig_chksum = chksum = *(uint32_t *)chksum_ptr;
    if (is_swapped)
        chksum = myflip(chksum);
    *(uint32_t *)chksum_ptr = 0;
    uint32_t calc = IS_CRC32C(page) ? crc32c(page, pagesize)
                                    : __ham_func4(page, pagesize);
    *verify_cksum = calc;
    *(uint32_t *)chksum_ptr = orig_chksum;
    return (calc == chksum) ? 1 : 0;
}
