/*
   Copyright 2020 Bloomberg Finance L.P.

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
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) &&          \
    !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
#define SQLITE_CORE 1
#endif

#include <pthread.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "sql.h"
#include <plhash_glue.h>
#include "timepart_systable.h"
#include "sqliteInt.h"

typedef struct table_entry {
    char *table;
    char *user;
    char *read;
    char *write;
    char *ddl;
} table_entry_t;

// clang-format off
#define TABLE_COLUMNS                                                          \
    CDB2_CSTRING, "tablename", -1, offsetof(table_entry_t, table),             \
    CDB2_CSTRING, "username", -1, offsetof(table_entry_t, user),               \
    CDB2_CSTRING, "READ", -1, offsetof(table_entry_t, read),                   \
    CDB2_CSTRING, "WRITE", -1, offsetof(table_entry_t, write),                 \
    CDB2_CSTRING, "DDL", -1, offsetof(table_entry_t, ddl),                     \
    SYSTABLE_END_OF_FIELDS
// clang-format on

typedef enum {
    PERM_BASE_TABLE,
    PERM_SYSTEM_TABLE,
    PERM_TIME_PARTITION,
} perm_obj_type_enum;

static void free_table_entries(void *data, int nrows)
{
    table_entry_t *rows = (table_entry_t *)data;
    if (!rows)
        return;

    for (int i = 0; i < nrows; i++) {
        free(rows[i].table);
        free(rows[i].user);
    }
    free(rows);
}

static void free_list(char **lst, int size)
{
    for (int i = 0; i < size; ++i) {
        free(lst[i]);
    }
    free(lst);
}

static int get_base_table_list(tran_type *trans, char ***table_list,
                               int *table_count)
{
    *table_count = 0;

    struct sql_thread *thd = pthread_getspecific(query_info_key);
    char *usr = thd->clnt->current_user.name;

    char **lst = calloc(thedb->num_dbs, sizeof(char *));
    if (!lst) {
        logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__, __LINE__);
        return -1;
    }

    int count = 0;

    /* List of tables accessible to the current user */
    for (int i = 0; i < thedb->num_dbs; ++i) {
        char *tbl;
        int err;

        tbl = thedb->dbs[i]->tablename;
        if (bdb_check_user_tbl_access_tran(thedb->bdb_env, trans, usr, tbl,
                                           ACCESS_READ, &err) != 0) {
            continue;
        }

        lst[count] = strdup(tbl);
        if (!lst[count]) {
            free_list(lst, count);
            return -1;
        }
        count++;
    }

    *table_list = lst;
    *table_count = count;

    return 0;
}

static int get_system_table_list(ez_systable_vtab *vtab, char ***table_list,
                                 int *table_count)
{
    *table_count = 0;

    char **lst = calloc(vtab->db->aModule.count, sizeof(char *));
    if (!lst) {
        logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__, __LINE__);
        return -1;
    }

    int count = 0;

    HashElem *systbl;
    for (systbl = sqliteHashFirst(&vtab->db->aModule); systbl;
         systbl = sqliteHashNext(systbl)) {
        struct Module *mod = sqliteHashData(systbl);
        if (!(mod->pModule->access_flag & CDB2_HIDDEN)) {
            lst[count] = strdup(systbl->pKey);
            if (!lst[count]) {
                free_list(lst, count);
                return -1;
            }

            count++;
        }
    }

    *table_list = lst;
    *table_count = count;

    return 0;
}

static int polulate_rows(tran_type *trans, void **data, int *nrows,
                         char **table_list, int table_count, char **user_list,
                         int user_count)
{

    *nrows = 0;

    table_entry_t *rows = calloc(((size_t)user_count) * table_count, sizeof(table_entry_t));
    if (!rows) {
        return -1;
    }

    int k = 0;

    for (int i = 0; i < table_count; ++i) {
        for (int j = 0; j < user_count; ++j) {
            char *tbl = table_list[i];
            char *usr = user_list[j];
            int err;
            rows[k].table = strdup(tbl);
            if (!rows[k].table) {
                logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__,
                       __LINE__);
                free_table_entries(rows, k);
                return -1;
            }

            rows[k].user = strdup(usr);
            if (!rows[k].user) {
                logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__,
                       __LINE__);
                free_table_entries(rows, k);
                return -1;
            }

            rows[k].read = YESNO(!bdb_check_user_tbl_access_tran(
                thedb->bdb_env, trans, usr, tbl, ACCESS_READ, &err));
            rows[k].write = YESNO(!bdb_check_user_tbl_access_tran(
                thedb->bdb_env, trans, usr, tbl, ACCESS_WRITE, &err));
            rows[k].ddl = YESNO(!bdb_check_user_tbl_access_tran(
                thedb->bdb_env, trans, usr, tbl, ACCESS_DDL, &err));
            k++;
        }
    }

    *data = rows;
    *nrows = k;

    return 0;
}

static int populate_rows(void **data, int *nrows, perm_obj_type_enum type,
                         ez_systable_vtab *vtab)
{
    int rc = SQLITE_OK;
    char **table_list = 0;
    int table_count = 0;
    char **user_list = 0;
    int user_count = 0;

    *nrows = 0;
    *data = NULL;

    tran_type *trans = curtran_gettran();
    if (!trans) {
        logmsg(LOGMSG_ERROR, "%s:%d cannot create transaction object\n",
               __func__, __LINE__);
        rc = -1;
        goto err;
    }

    switch (type) {
    case PERM_BASE_TABLE:
        /* List of tables accessible to the current user. */
        if ((rc = get_base_table_list(trans, &table_list, &table_count))) {
            curtran_puttran(trans);
            goto err;
        }
        break;
    case PERM_SYSTEM_TABLE:
        /* List of system tables */
        if ((rc = get_system_table_list(vtab, &table_list, &table_count))) {
            curtran_puttran(trans);
            goto err;
        }
        break;
    case PERM_TIME_PARTITION:
        /* Collect all time partitions accessible by current user. */
        if ((rc = timepart_systable_timepartpermissions_collect(
                 (void **)&table_list, &table_count))) {

            curtran_puttran(trans);
            goto err;
        }

        break;
    default:
        logmsg(LOGMSG_FATAL, "%s:%d invalid object type\n", __func__, __LINE__);
        abort();
    }

    /* List of users */
    bdb_user_get_all_tran(trans, &user_list, &user_count);

    rc = polulate_rows(trans, data, nrows, table_list, table_count, user_list,
                       user_count);
    curtran_puttran(trans);

    if (rc) {
        goto err;
    }

    free_list(table_list, table_count);
    free_list(user_list, user_count);
    return 0;

err:
    free_list(table_list, table_count);
    free_list(user_list, user_count);
    free_table_entries(*data, *nrows);

    return rc;
}

static int populate_table_permissions(void **data, int *nrows)
{
    return populate_rows(data, nrows, PERM_BASE_TABLE, NULL);
}

static int populate_systab_permissions(ez_systable_vtab *vtab, void **data,
                                       int *nrows)
{
    return populate_rows(data, nrows, PERM_SYSTEM_TABLE, vtab);
}

static int populate_timepart_permissions(void **data, int *nrows)
{
    return populate_rows(data, nrows, PERM_TIME_PARTITION, NULL);
}

sqlite3_module systblTablePermissionsModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables",
};

int systblTablePermissionsInit(sqlite3 *db)
{
    return create_system_table(db, "comdb2_tablepermissions",
                               &systblTablePermissionsModule,
                               populate_table_permissions, free_table_entries,
                               sizeof(table_entry_t), TABLE_COLUMNS);
}

sqlite3_module systblSystabPermissionsModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables",
};

int systblSystabPermissionsInit(sqlite3 *db)
{
    return create_system_table_v2(
        db, "comdb2_systablepermissions", &systblSystabPermissionsModule,
        populate_systab_permissions, free_table_entries, sizeof(table_entry_t),
        TABLE_COLUMNS);
}

sqlite3_module systblTimepartPermissionsModule = {
    .access_flag = CDB2_ALLOW_ALL,
    .systable_lock = "comdb2_tables",
};

int systblTimepartPermissionsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_timepartpermissions", &systblTimepartPermissionsModule,
        populate_timepart_permissions, free_table_entries,
        sizeof(table_entry_t), TABLE_COLUMNS);
}

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
