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

#ifndef INCLUDED_DB_WRAP
#define INCLUDED_DB_WRAP

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "ar_glue.h"

#define DEFAULT_PAGE_SIZE 4096

typedef struct dbfile {
    const char *filename;
    size_t pagesize;
    size_t offset;
    size_t bufsize;
    DBTYPE type;
    uint8_t metaflags;
    uint8_t *pagebuf;
    int is_encrypted : 1;
    int is_swapped : 1;
    ssize_t chunk_size;
} dbfile_info;

typedef int (*writer_cb)(void *, uint8_t *, size_t, size_t);

dbfile_info *dbfile_init(dbfile_info *, const char *filename);
void dbfile_deinit(dbfile_info *);
const char *dbfile_filename(dbfile_info *);
size_t dbfile_pagesize(dbfile_info *);
ssize_t dbfile_get_chunk_size(dbfile_info *f);
void dbfile_set_chunk_size(dbfile_info *f, ssize_t chunk_size);

int dbfile_is_encrypted(dbfile_info *);
int dbfile_is_swapped(dbfile_info *);
int dbfile_is_checksummed(dbfile_info *);
int dbfile_is_sparse(dbfile_info *);

int verify_checksum(uint8_t *page, size_t pagesize, int is_encrypted,
                    int is_swapped, uint32_t *verify_cksum);
int fileid(const char *, int, DBMETA *);
uint32_t myflip(uint32_t in);
int recognize_data_file(const char *filename, uint8_t *is_data_file,
                        uint8_t *is_queue_file, uint8_t *is_queuedb_file,
                        char **out_table_name);
int read_write_file(dbfile_info *file, void *writer_ctx, writer_cb writer);

#ifdef __cplusplus
}
#endif

#endif /* INCLUDED_DB_WRAP */
