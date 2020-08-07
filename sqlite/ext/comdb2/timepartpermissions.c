/*
**
** Vtables interface for Schema Tables.
**
** Though this is technically an extension, currently it must be
** built as part of SQLITE_CORE, as comdb2 does not support
** run time extensions at this time.
**
** For a little while we had to use our own "fake" tables, because
** eponymous system tables did not exist. Now that they do, we
** have moved schema tables to their own extension.
**
** We have piggy backed off of SQLITE_BUILDING_FOR_COMDB2 here, though
** a new #define would also suffice.
*/
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) &&          \
    !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
#define SQLITE_CORE 1
#endif

#include <stdlib.h>
#include <string.h>

#include "comdb2.h"
#include "sql.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "timepart_systable.h"

/* tp_permissions_cursor is a subclass of sqlite3_vtab_cursor which serves
** as the underlying cursor to enumerate the rows in this vtable.
*/
typedef struct tp_permissions_cursor {
    sqlite3_vtab_cursor base; /* Base class - must be first */
    int iUser;
    int iTimePartition;
    char **ppUsers;
    int nUsers;
    char **ppTimePartitions;
    int nTimePartitions;
} tp_permissions_cursor;

typedef struct {
    sqlite3_vtab base; /* Base class - must be first */
    sqlite3 *db;       /* To access registered system tables */
} systbl_tp_permissions_vtab;

static int tp_permissions_connect(sqlite3 *db, void *pAux, int argc,
                                  const char *const *argv,
                                  sqlite3_vtab **ppVtab, char **pErr)
{
    systbl_tp_permissions_vtab *pNew = NULL;
    int rc;

    /* Column numbers */
#define COLUMN_TABLE 0
#define COLUMN_USER 1
#define COLUMN_READ 2
#define COLUMN_WRITE 3
#define COLUMN_DDL 4

    rc = sqlite3_declare_vtab(db, "CREATE TABLE comdb2_timepartpermissions"
                                  "(name,username,READ,WRITE,DDL);");
    if (rc == SQLITE_OK) {
        pNew = sqlite3_malloc(sizeof(systbl_tp_permissions_vtab));
        if (pNew == 0)
            return SQLITE_NOMEM;
        memset(pNew, 0, sizeof(systbl_tp_permissions_vtab));
        pNew->db = db;
    }

    *ppVtab = (sqlite3_vtab *)pNew;

    return rc;
}

/*
** Destructor for sqlite3_vtab objects.
*/
static int tp_permissions_disconnect(sqlite3_vtab *pVtab)
{
    sqlite3_free(pVtab);
    return SQLITE_OK;
}

static int tp_permissions_open(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor)
{
    tp_permissions_cursor *pCur = sqlite3_malloc(sizeof(*pCur));
    if (pCur == 0)
        return SQLITE_NOMEM;
    memset(pCur, 0, sizeof(*pCur));

    /* Collect all time partitions accessible by current user. */
    timepart_systable_timepartpermissions_collect(
        (void **)&pCur->ppTimePartitions, &pCur->nTimePartitions);

    rdlock_schema_lk();
    bdb_user_get_all(&pCur->ppUsers, &pCur->nUsers);
    unlock_schema_lk();

    *ppCursor = &pCur->base;
    return SQLITE_OK;
}

static int tp_permissions_close(sqlite3_vtab_cursor *cur)
{
    int i;
    tp_permissions_cursor *pCur = (tp_permissions_cursor *)cur;
    /* Memory for users[] is owned by bdb */
    for (i = 0; i < pCur->nUsers; ++i) {
        free(pCur->ppUsers[i]);
    }
    free(pCur->ppUsers);
    timepart_systable_timepartpermissions_free(pCur->ppTimePartitions,
                                               pCur->nTimePartitions);
    sqlite3_free(pCur);
    return SQLITE_OK;
}

static int tp_permissions_next(sqlite3_vtab_cursor *cur)
{
    tp_permissions_cursor *pCur = (tp_permissions_cursor *)cur;
    ++pCur->iUser;
    if (pCur->iUser >= pCur->nUsers) {
        pCur->iUser = 0;
        ++pCur->iTimePartition;
    }
    return SQLITE_OK;
}

static int tp_permissions_column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                                 int i)
{
    tp_permissions_cursor *pCur = (tp_permissions_cursor *)cur;
    char *tbl = pCur->ppTimePartitions[pCur->iTimePartition];
    char *usr = pCur->ppUsers[pCur->iUser];
    switch (i) {
    case COLUMN_TABLE: {
        sqlite3_result_text(ctx, tbl, -1, NULL);
        break;
    }
    case COLUMN_USER: {
        sqlite3_result_text(ctx, usr, -1, NULL);
        break;
    }
    case COLUMN_READ:
    case COLUMN_WRITE:
    case COLUMN_DDL: {
        int access, err;
        if (i == COLUMN_READ) {
            access = ACCESS_READ;
        } else if (i == COLUMN_WRITE) {
            access = ACCESS_WRITE;
        } else {
            access = ACCESS_DDL;
        }
        sqlite3_result_text(
            ctx,
            YESNO(!bdb_check_user_tbl_access(NULL, usr, tbl, access, &err)), -1,
            SQLITE_STATIC);
        break;
    }
    }
    return SQLITE_OK;
}

static int tp_permissions_rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid)
{
    tp_permissions_cursor *pCur = (tp_permissions_cursor *)cur;
    *pRowid = pCur->iUser;
    return SQLITE_OK;
}

static int tp_permissions_eof(sqlite3_vtab_cursor *cur)
{
    tp_permissions_cursor *pCur = (tp_permissions_cursor *)cur;
    if (pCur->nUsers == 0)
        return 1;
    return pCur->iTimePartition >= pCur->nTimePartitions;
}

/*
** This method is called to "rewind" the series_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to xColumn() or xRowid() or xEof().
*/
static int tp_permissions_filter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                                 const char *idxStr, int argc,
                                 sqlite3_value **argv)
{
    tp_permissions_cursor *pCur = (tp_permissions_cursor *)pVtabCursor;

    pCur->iUser = 0;
    pCur->iTimePartition = 0;
    return SQLITE_OK;
}

/*
** There is no way to really take advantage of this at the moment.
** The output of this table is a mostly unordered list of strings.
*/
static int tp_permissions_best_index(sqlite3_vtab *tab,
                                     sqlite3_index_info *pIdxInfo)
{
    return SQLITE_OK;
}

const sqlite3_module systblTableTPPermissionsModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    tp_permissions_connect,    /* xConnect */
    tp_permissions_best_index, /* xBestIndex */
    tp_permissions_disconnect, /* xDisconnect */
    0,                         /* xDestroy */
    tp_permissions_open,       /* xOpen - open a cursor */
    tp_permissions_close,      /* xClose - close a cursor */
    tp_permissions_filter,     /* xFilter - configure scan constraints */
    tp_permissions_next,       /* xNext - advance a cursor */
    tp_permissions_eof,        /* xEof - check for end of scan */
    tp_permissions_column,     /* xColumn - read data */
    tp_permissions_rowid,      /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindMethod */
    0,                         /* xRename */
    0,                         /* xSavepoint */
    0,                         /* xRelease */
    0,                         /* xRollbackTo */
    0,                         /* xShadowName */
    .access_flag = CDB2_ALLOW_ALL,
};

#endif /* (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2))       \
          && !defined(SQLITE_OMIT_VIRTUALTABLE) */
