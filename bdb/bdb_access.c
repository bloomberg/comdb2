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
#include "cdb2_constants.h"

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

extern int gbl_allow_user_schema;
extern int gbl_uses_password;

int bdb_check_user_tbl_access_tran(bdb_state_type *bdb_state, tran_type *tran,
                                   char *user, char *table, int access_type,
                                   int *bdberr)
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
            rc = bdb_tbl_access_read_get(bdb_state, tran, table, user, bdberr);
        }
        if (rc != 0 &&
            (access_type == ACCESS_WRITE || access_type == ACCESS_READ)) {
            rc = bdb_tbl_access_write_get(bdb_state, tran, table, user, bdberr);
        }
        if (rc != 0 &&
            (access_type == ACCESS_DDL || access_type == ACCESS_WRITE ||
             access_type == ACCESS_READ)) {
            /* Check for DDL privilege on table */
            rc = bdb_tbl_op_access_get(bdb_state, tran, 0, table, user, bdberr);
        }
        if (rc != 0) {
            /* Check for OP privilege */
            rc = bdb_tbl_op_access_get(bdb_state, tran, 0, "", user, bdberr);
        }
    }
    return rc;
}

static int bdb_del_user_tbl_access(bdb_state_type *bdb_state, tran_type *tran,
                                   const char *user, const char *table_name)
{
    int rc = 0;
    int bdberr;

    if ((rc = bdb_tbl_op_access_delete(bdb_state, tran, 0, table_name,
                                       user, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR,
               "error deleting user access information (user: %s, table: %s, "
               "rc: %d, bdberr: %d)\n",
               user, table_name, rc, bdberr);
        return rc;
    }

    if ((rc = bdb_tbl_access_read_delete(bdb_state, tran, table_name, user,
                                         &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR,
               "error deleting user access information (user: %s, table: %s, "
               "rc: %d, bdberr: %d)\n",
               user, table_name, rc, bdberr);
        return rc;
    }
    if ((rc = bdb_tbl_access_write_delete(bdb_state, tran, table_name, user,
                                          &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR,
               "error deleting user access information (user: %s, table: %s, "
               "rc: %d, bdberr: %d)\n",
               user, table_name, rc, bdberr);
        return rc;
    }

    return rc;
}

/* Delete all access permissions related to a specific table. */
int bdb_del_all_table_access(bdb_state_type *bdb_state, tran_type *tran,
                             const char *table_name)
{
    int rc = 0;
    char **users;
    int nUsers;

    if ((rc = bdb_user_get_all_tran(tran, &users, &nUsers)) != 0) {
        logmsg(LOGMSG_ERROR, "error retrieving user list (rc: %d)\n", rc);
        return rc;
    }

    for (int i = 0; i < nUsers; ++i) {
        if ((rc = bdb_del_user_tbl_access(bdb_state, tran, users[i],
                                          table_name))) {
            break;
        }
    }

    for (int i = 0; i != nUsers; ++i)
        free(users[i]);
    free(users);

    return rc;
}

/* Delete all access permissions for a specific user. */
int bdb_del_all_user_access(bdb_state_type *bdb_state, tran_type *tran,
                            const char *user)
{
    int rc = 0;
    int dbnums[MAX_NUM_TABLES];
    char *tblnames[MAX_NUM_TABLES];
    int numtbls;
    int bdberr;

    if ((rc = bdb_llmeta_get_tables(tran, tblnames, dbnums, MAX_NUM_TABLES,
                                    &numtbls, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR,
               "error retrieving table list (rc: %d, bdberr: %d)\n", rc,
               bdberr);
        return rc;
    }

    for (int i = 0; i < numtbls; ++i) {
        if ((rc =
                 bdb_del_user_tbl_access(bdb_state, tran, user, tblnames[i]))) {
            break;
        }
    }

    return rc;
}

int bdb_check_user_tbl_access(bdb_state_type *bdb_state, char *user,
                              char *table, int access_type, int *bdberr)
{
    return bdb_check_user_tbl_access_tran(bdb_state, NULL, user, table,
                                          access_type, bdberr);
}

int gbl_create_dba_user = 1;

int bdb_create_dba_user(bdb_state_type *bdb_state)
{
    int rc;
    int bdberr;

    if (!gbl_create_dba_user) {
        return 0;
    }

    rc = bdb_user_exists(NULL, DEFAULT_DBA_USER);
    if (rc == -1) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to retrieve user information\n",
               __func__, __LINE__);
        return 1;
    } else if (rc == 1) {
        logmsg(LOGMSG_USER, "DBA user '%s' already exists\n", DEFAULT_DBA_USER);
        return 0;
    }

    /* Set the user */
    rc = bdb_user_password_set(NULL, DEFAULT_DBA_USER, DEFAULT_DBA_PASSWORD);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to create the DBA user\n", __func__,
               __LINE__);
        rc = 1;
        goto done;
    }

    /* Give them OP privilege */
    rc = bdb_tbl_op_access_set(bdb_state, NULL, 0, "", DEFAULT_DBA_USER, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to grant OP privileges to the DBA user\n",
               __func__, __LINE__);
        rc = 1;
        goto done;
    }

    logmsg(LOGMSG_USER, "DBA user '%s' created\n", DEFAULT_DBA_USER);
    rc = 0;

done:
    return rc;
}
