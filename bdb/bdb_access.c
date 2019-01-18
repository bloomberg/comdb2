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

#include <strings.h>
#include <string.h>
#include <assert.h>
#include "bdb_int.h"
#include "bdb_access.h"

#include <logmsg.h>

/* keep MACHNAMELEN in sync with USERNAME length */
#define MACHNAMELEN 16

#define MAX_HOSTS 8192
#define BITS_PER_HOST 2
#define HOSTS_PER_CHAR (8 / BITS_PER_HOST)
const unsigned char HOST_MASKS[] = {0x03, 0x0C, 0x30, 0xC0};

#define ACC_DEFAULT_DENY 0
#define ACC_DEFAULT_ALLOW 1

#define HOST_BITS_GET(ctrl, hnum)                                              \
    ((((ctrl)->hosts_cache[(hnum) / HOSTS_PER_CHAR]) >>                        \
      (((hnum) % HOSTS_PER_CHAR) * BITS_PER_HOST)) &                           \
     HOST_MASKS[0])

#define HOST_BITS_SET(ctrl, hnum, val)                                         \
    (ctrl)->hosts_cache[(hnum) / HOSTS_PER_CHAR] =                             \
        (((ctrl)->hosts_cache[(hnum) / HOSTS_PER_CHAR] &                       \
          (~HOST_MASKS[(hnum) % HOSTS_PER_CHAR])) |                            \
         ((val) << (((hnum) % HOSTS_PER_CHAR) * BITS_PER_HOST)))

struct bdb_access_tag {
    int default_access;
    char hosts_cache[MAX_HOSTS * BITS_PER_HOST / sizeof(char)];
};

static int bdb_access_table_by_mach_get_disk(bdb_state_type *bdb_state,
                                             tran_type *tran, char *table,
                                             int hostnum, int *bdberr);
static int bdb_access_table_by_mach_put_disk(bdb_state_type *bdb_state,
                                             tran_type *tran, char *table,
                                             int hostnum, int allow,
                                             int *bdberr);

bdb_access_t *bdb_access_create(bdb_state_type *bdb_state, int *bdberr)
{
    int i = 0;

    for (i = 0; i < bdb_state->numchildren; i++) {
        if (bdb_state->children[i]) {
            if (bdb_access_create(bdb_state->children[i], bdberr) == NULL)
                return NULL;
        }
    }

    *bdberr = 0;
    if (bdb_state->access)
        return bdb_state->access;

    bdb_state->access = calloc(1, sizeof(bdb_access_t));
    if (!bdb_state->access)
        *bdberr = BDBERR_MALLOC;
    return bdb_state->access;
}

void bdb_access_destroy(bdb_state_type *bdb_state)
{
    if (bdb_state->access) {
        free(bdb_state->access);
        bdb_state->access = NULL;
    }
}

static int bdb_access_tbl_by_mach_get_int(bdb_state_type *bdb_state,
                                          tran_type *tran, char *table,
                                          int hostnum, int *bdberr)
{
    bdb_access_t *access = bdb_state->access;
    int bits;
    int rc;

    *bdberr = 0;

    /* if access control is not set, allow */
    if (!access)
        return ACCESS_READ | ACCESS_WRITE;

    bits = HOST_BITS_GET(access, hostnum);

    if (bits == ACCESS_INVALID) {
        /* slow path, lookup llmeta */
        rc = bdb_access_table_by_mach_get_disk(bdb_state, tran, table, hostnum,
                                               bdberr);
        if (rc < 0) {
            /* do not allow on error */
            logmsg(LOGMSG_ERROR, "%s:%d error rc=%d\n", __FILE__, __LINE__, rc);
            return -1;
        }
        HOST_BITS_SET(access, hostnum, rc);

        bits = HOST_BITS_GET(access, hostnum);
    }

    assert(bits != ACCESS_INVALID);

    return bits;
}

int bdb_access_tbl_read_by_mach_get(bdb_state_type *bdb_state, tran_type *tran,
                                    char *table, int hostnum, int *bdberr)
{
    int bits =
        bdb_access_tbl_by_mach_get_int(bdb_state, tran, table, hostnum, bdberr);

    if (bits < 0)
        return bits;

    return (bits & ACCESS_READ) != 0;
}

int bdb_access_tbl_write_by_mach_get(bdb_state_type *bdb_state, tran_type *tran,
                                     char *table, int hostnum, int *bdberr)
{
    int bits =
        bdb_access_tbl_by_mach_get_int(bdb_state, tran, table, hostnum, bdberr);

    if (bits < 0)
        return bits;

    return (bits & ACCESS_WRITE) != 0;
}

static int bdb_access_table_by_mach_get_disk(bdb_state_type *bdb_state,
                                             tran_type *tran, char *table,
                                             int hostnum, int *bdberr)
{
    char machname[MACHNAMELEN];
    int bits = 0;
    int rc = 0;

    snprintf(machname, sizeof(machname), "%d", hostnum);

    rc = bdb_tbl_access_write_get(bdb_state, tran, table, machname, bdberr);
    if (rc == 0)
        bits |= ACCESS_WRITE;

    rc = bdb_tbl_access_read_get(bdb_state, tran, table, machname, bdberr);
    if (rc == 0)
        bits |= ACCESS_READ;

    return (bits) ? bits : -1;
}

static int bdb_access_table_by_mach_put_disk(bdb_state_type *bdb_state,
                                             tran_type *tran, char *table,
                                             int hostnum, int allow,
                                             int *bdberr)
{

    char machname[MACHNAMELEN];
    int rc = 0;

    snprintf(machname, sizeof(machname), "%d", hostnum);

    if (allow & ~(ACCESS_READ | ACCESS_WRITE)) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (allow & ACCESS_WRITE) {
        rc = bdb_tbl_access_write_set(bdb_state, tran, table, machname, bdberr);
        if (rc)
            return -1;
    }
    if (allow & ACCESS_READ) {
        rc = bdb_tbl_access_read_set(bdb_state, tran, table, machname, bdberr);
        if (rc)
            return -1;
    }

    return 0;
}

/**
 * Sets the right to write for (table, hostname)
 *
 */
int bdb_access_tbl_write_by_mach_set(bdb_state_type *bdb_state, tran_type *tran,
                                     char *table, int hostnum, int *bdberr)
{
    return bdb_access_table_by_mach_put_disk(bdb_state, tran, table, hostnum,
                                             ACCESS_WRITE, bdberr);
}

/**
 * Sets the right to read for (table, hostname)
 *
 */
int bdb_access_tbl_read_by_mach_set(bdb_state_type *bdb_state, tran_type *tran,
                                    char *table, int hostnum, int *bdberr)
{
    return bdb_access_table_by_mach_put_disk(bdb_state, tran, table, hostnum,
                                             ACCESS_READ, bdberr);
}

/**
 * Invalidate the cache when deleting
 *
 */
void bdb_access_tbl_invalidate(bdb_state_type *bdb_state)
{
    if (!bdb_state->access)
        return;

    /* dummy implementation for now */
    bzero(bdb_state->access->hosts_cache,
          sizeof(bdb_state->access->hosts_cache));
}

int gbl_allow_user_schema;
int gbl_uses_password;

int bdb_check_user_tbl_access(bdb_state_type *bdb_state, char *user,
                              char *table, int access_type, int *bdberr)
{
    int rc = 0;
    if (gbl_uses_password) {
        rc = -1;
        if (gbl_allow_user_schema) {
            char *username = strstr(table, "@");
            if (username && strcmp(username + 1, user) == 0) {
                return 0;
            }
        }
        if (access_type == ACCESS_READ) {
            rc = bdb_tbl_access_read_get(bdb_state, NULL, table, user, bdberr);
        }
        if (rc != 0 &&
            (access_type == ACCESS_WRITE || access_type == ACCESS_READ)) {
            rc = bdb_tbl_access_write_get(bdb_state, NULL, table, user, bdberr);
        }
        if (rc != 0 &&
            (access_type == ACCESS_DDL || access_type == ACCESS_WRITE ||
             access_type == ACCESS_READ)) {
            rc = bdb_tbl_op_access_get(bdb_state, NULL, 0, table, user, bdberr);
        }
        if (rc != 0) {
            rc = bdb_tbl_op_access_get(bdb_state, NULL, 0, "", user, bdberr);
        }
    }
    return rc;
}
