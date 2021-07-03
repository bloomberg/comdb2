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

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <poll.h>
#include "cdb2_constants.h"
#include "db_wrap.h"
#include "db_checksum.h"

#define MIN(A,B) (A < B) ? A : B
#define MAX_BUF_SIZE 4 * 1024 * 1024

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

    file->filename = filename;

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
                                    : comdb2_ham_func4(page, pagesize);
    *verify_cksum = calc;
    *(uint32_t *)chksum_ptr = orig_chksum;
    return (calc == chksum) ? 1 : 0;
}

// Determine if the given filename looks like a table or queue file. If it does
// then return 1 and set the is_ flags appropriately, and put the name of the
// object in out_table_name.
int recognize_data_file(const char *filename, uint8_t *is_data_file,
                        uint8_t *is_queue_file, uint8_t *is_queuedb_file,
                        char **out_table_name)
{
    char *dot_pos = strchr(filename, '.');
    if(dot_pos == NULL) {
        return 0;
    }

    char *ext = dot_pos + 1;
    size_t len = dot_pos - filename;

    // queues are the same whether we are llmeta or not
    if((strcmp(ext, "queue")) == 0) {
        *is_queue_file = 1;
    } else if((strcmp(ext, "queuedb")) == 0) {
        *is_queuedb_file = 1;
    }
    // comdb2 seems to use metalite.dta even in llmeta mode..
    // copy it to stop it trying to create them on startup.
    // Same seems to apply to things like freerec; conversion to llmeta
    // mode doesn't kill off all these exotic file types so occasionally
    // you come across a db that requires them.
    else if (((strcmp(ext, "metalite.dta")) == 0) ||
        ((strcmp(ext, "freerec")) == 0) ||
        ((strcmp(ext, "freerecq")) == 0) ||
        ((strcmp(ext, "meta.dta")) == 0) ||
        ((strcmp(ext, "meta.ix0")) == 0) ||
        ((strcmp(ext, "meta.freerec")) == 0))
    {
        *is_data_file = 1;
    }
    // Look for *.data*, *.index and *.blob*
    // Also strip out the 16 digit hex suffix that comes after the table
    // name.
    else if((dot_pos - filename) > 17 && *(dot_pos - 17) == '_' &&
       (((strcmp(ext, "index")) == 0) ||
        ((strncmp(ext, "data", 4)) == 0) ||
        ((strncmp(ext, "blob", 4)) == 0))) {
        len = dot_pos - 17 - filename;
        *is_data_file = 1;
    } else {
        /* It is not one of the data files. */
        return 0;
    }

    /* It is a data file. */
    memcpy(*out_table_name, filename, MIN(len, MAXTABLELEN));
    (*out_table_name)[len] = '\0';
    return 1;
}

// Part of the logic copied from serialise.cpp: serialise_file()
int read_write_file(dbfile_info *file, void *writer_ctx, writer w)
{
    int flags = O_RDONLY/*|O_DIRECT*/;
    int fd = -1;

    // Ensure large file support
    assert(sizeof(off_t) == 8);

    fd = open(file->filename, flags);
    if (fd == -1) {
        return 1;
    }

    struct stat st;
    if(fstat(fd, &st) == -1) {
        return -1;
    }

    // Ignore special files
    if(!S_ISREG(st.st_mode)) {
        return -1;
    }

    // Read the file a page at a time and copy to the output.
    // Use a large buffer if possible
    size_t pagesize = dbfile_pagesize(file);
    if(pagesize == 0) {
        pagesize = 4096;
    }

    size_t bufsize = pagesize;
    int64_t filesize = 0;

    while((bufsize << 1) <= MAX_BUF_SIZE) {
        bufsize <<= 1;
    }

    uint8_t *pagebuf = NULL;
    off_t bytesleft = st.st_size;

    char *buffer = malloc(st.st_size);
    if (buffer == NULL) {
        return -1;
    }
    char *buf_ptr = buffer;

#if ! defined  ( _SUN_SOURCE ) && ! defined ( _HP_SOURCE )
        if(posix_memalign((void**) &pagebuf, 512, bufsize))
            return -1;
#else
        pagebuf = (uint8_t*) memalign(512, bufsize);
#endif

    while(bytesleft > 0) {
        unsigned long long nbytes = bytesleft > bufsize ? bufsize : bytesleft;

        ssize_t bytesread = read(fd, &pagebuf[0], nbytes);
        if(bytesread <= 0) {
            return -1;
        }

        filesize += bytesread;

        if (dbfile_is_checksummed(file)) {
            // Save current offset
            const off_t offset = lseek(fd, 0, SEEK_CUR);
            if (offset == (off_t) -1) {
                return -1;
            }

            int retry = 5;
            ssize_t n = 0;

            while (n < bytesread && retry) {
                uint32_t verify_cksum;

                if ((verify_checksum(pagebuf + n, pagesize,
                                     dbfile_is_encrypted(file),
                                     dbfile_is_swapped(file),
                                     &verify_cksum)) == 1) {
                    // checksum verified
                    n += pagesize;
                    retry = 5;

                    continue;
                }

                // Partial page read. Read the page again to see if it passes
                // checksum verification.
                if (--retry == 0) {
                    //giving up on this page
                    return -1;
                }

                // wait 500ms before reading page again
                poll(0, 0, 500);

                // rewind and read the page again
                off_t rewind = offset - (bytesread - n);
                rewind = lseek(fd, rewind, SEEK_SET);
                if (rewind == (off_t) -1) {
                    return -1;
                }

                ssize_t nread, totalread = 0;
                while (totalread < pagesize) {
                    nread = read(fd, &pagebuf[0] + n + totalread,
                                 pagesize - totalread);
                    if (nread <= 0) {
                        return -1;
                    }
                    totalread += nread;
                }
            }

            // Restore to original offset
            if (offset != lseek(fd, offset, SEEK_SET)) {
                return -1;
            }
        }

        w(writer_ctx, pagebuf, bytesread);
        buf_ptr += bytesread;
        bytesleft -= bytesread;
    }

    return 0;
}
