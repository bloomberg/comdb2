#include <string.h>
#include <poll.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <berkdb/dbinc/queue.h>
#include <schemachange.h>
#include <sc_struct.h>
#include <strbuf.h>
#include <sqliteInt.h>
#include <comdb2build.h>
#include <comdb2vdbe.h>
#include <trigger.h>
#include <sqlglue.h>

struct dbtable;
struct dbtable *getqueuebyname(const char *);
int bdb_get_sp_get_default_version(const char *, int *);

#define COMDB2_DEFAULT_CONSUMER 2

static int comdb2LocateSP(Parse *p, char *sp)
{
    char *ver = NULL;
    int rc = bdb_get_default_versioned_sp(sp, &ver);
    free(ver);
    if (rc >= 0) return 1; // client-versioned default

    int bdberr;
    if (bdb_get_sp_get_default_version(sp, &bdberr) >= 0) return 1; // old-style default version

    return 0; // no such procedure or no default version for procedure
}

enum ops { del = 0x01, ins = 0x02, upd = 0x04 };

typedef struct columnevent {
    const char *col;
    int event;
    LIST_ENTRY(columnevent) link;
} ColumnEvent;

typedef struct {
    LIST_HEAD(, columnevent) head;
} ColumnEventList;

static ColumnEvent *getcol(ColumnEventList *list, const char *col)
{
    ColumnEvent *e = NULL;
    LIST_FOREACH(e, &list->head, link) {
        if (strcmp(e->col, col) == 0) {
            return e;
        }
    }
    e = malloc(sizeof(ColumnEvent));
    e->col = col;
    e->event = 0;
    LIST_INSERT_HEAD(&list->head, e, link);
    return e;
}

static void add_watched_cols(int type, Table *table, Cdb2TrigEvent *event, ColumnEventList *list)
{
    if (event->cols) {
        for (int i = 0; i < event->cols->nId; ++i) {
            ColumnEvent *ce = getcol(list, event->cols->a[i].zName);
            ce->event |= type;
        }
    } else {
        for (int i = 0; i < table->nCol; ++i) {
            ColumnEvent *ce = getcol(list, table->aCol[i].zName);
            ce->event |= type;
        }
    }
}

Cdb2TrigEvents *comdb2AddTriggerEvent(Parse *pParse, Cdb2TrigEvents *A, Cdb2TrigEvent *B)
{
    if (A == NULL) {
        A = sqlite3DbMallocZero(pParse->db, sizeof(Cdb2TrigEvents));
    }
    Cdb2TrigEvent *e = NULL;
    const char *type = NULL;
    switch (B->op) {
    case TK_DELETE: e = &A->del; type = "delete"; break;
    case TK_INSERT: e = &A->ins; type = "insert"; break;
    case TK_UPDATE: e = &A->upd; type = "update"; break;
    default: sqlite3ErrorMsg(pParse, "%s: bad op", __func__, B->op); return NULL;
    }
    if (B->op == e->op) {
        sqlite3DbFree(pParse->db, A);
        sqlite3ErrorMsg(pParse, "%s condition repeated", type);
        return NULL;
    }
    e->op = B->op;
    e->cols = B->cols;
    return A;
}

Cdb2TrigTables *comdb2AddTriggerTable(Parse *parse, Cdb2TrigTables *tables, SrcList *tbl, Cdb2TrigEvents *events)
{
    Table *table;
    if ((table = sqlite3LocateTableItem(parse, 0, &tbl->a[0])) == NULL) {
        sqlite3ErrorMsg(parse, "no such table:%s", tbl->a[0].zName);
        return NULL;
    }
    Cdb2TrigTables *tmp;
    const char *name = table->zName;
    if (tables) {
        tmp = tables;
        while (tmp) {
            if (strcmp(tmp->table->zName, name) == 0) {
                sqlite3ErrorMsg(parse, "trigger already specified table:%s", name);
                return NULL;
            }
            tmp = tmp->next;
        }
    }
    tmp = sqlite3DbMallocRaw(parse->db, sizeof(Cdb2TrigTables));
    if (tmp == NULL) {
        sqlite3ErrorMsg(parse, "malloc failED");
        return NULL;
    }
    tmp->table = table;
    tmp->events = events;
    tmp->next = tables;
    return tmp;
}

void comdb2CreateTrigger(Parse *parse, int consumer, int seq, Token *proc, Cdb2TrigTables *tbl)
{
    if (comdb2IsPrepareOnly(parse))
        return;

    if (comdb2IsDryrun(parse)) {
        sqlite3ErrorMsg(parse, "DRYRUN not supported for this operation");
        parse->rc = SQLITE_MISUSE;
        return;
    }
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, consumer ? SQLITE_CREATE_LUA_CONSUMER :
                             SQLITE_CREATE_LUA_TRIGGER, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (!tbl) {
        sqlite3ErrorMsg(parse, "invalid table name");
        return;
    }

    if (comdb2AuthenticateUserOp(parse))
        return;

    char spname[MAX_SPNAME];

    if (comdb2TokenToStr(proc, spname, sizeof(spname))) {
        sqlite3ErrorMsg(parse, "procedure name is too long");
        return;
    }

    Q4SP(qname, spname);
    if (getqueuebyname(qname)) {
        sqlite3ErrorMsg(parse, "%s:%s already exists", consumer ? "consumer" : "trigger", spname);
        return;
    }

    if (consumer == COMDB2_DEFAULT_CONSUMER) {
        if (tbl->next) {
            sqlite3ErrorMsg(parse, "cannot create default consumer for multiple tables");
            return;
        }
    } else if (!comdb2LocateSP(parse, spname)) {
        sqlite3ErrorMsg(parse, "no such procedure: %s", spname);
        return;
    }

    strbuf *s = strbuf_new();
    while (tbl) {
        Table *table = tbl->table;
        Cdb2TrigEvents *events = tbl->events;
        tbl = tbl->next;
        ColumnEventList celist;
        LIST_INIT(&celist.head);
        if (events->del.op == TK_DELETE) {
            add_watched_cols(del, table, &events->del, &celist);
        }
        if (events->ins.op == TK_INSERT) {
            add_watched_cols(ins, table, &events->ins, &celist);
        }
        if (events->upd.op == TK_UPDATE) {
            add_watched_cols(upd, table, &events->upd, &celist);
        }
        strbuf_appendf(s, "table %s\n", table->zName);
        ColumnEvent *prev = NULL, *ce = NULL;
        LIST_FOREACH(ce, &celist.head, link) {
            strbuf_appendf(s, "field %s", ce->col);
            if (ce->event & del) {
                strbuf_append(s, " del");
            }
            if (ce->event & ins) {
                strbuf_append(s, " add");
            }
            if (ce->event & upd) {
                strbuf_append(s, " pre_upd post_upd");
            }
            strbuf_append(s, "\n");
            free(prev);
            prev = ce;
        }
        free(prev);
    }

    char method[64];
    sprintf(method, "dest:%s:%s", consumer ? "dynlua" : "lua", spname);

    // trigger add table:qname dest:method
    struct schema_change_type *sc = new_schemachange_type();
    sc->kind = SC_ADD_TRIGGER;
    sc->persistent_seq = seq;
    strcpy(sc->tablename, qname);
    struct dest *d = malloc(sizeof(struct dest));
    d->dest = strdup(method);
    listc_abl(&sc->dests, d);
    sc->newcsc2 = strbuf_disown(s);
    strbuf_free(s);
    Vdbe *v = sqlite3GetVdbe(parse);

    if (consumer == COMDB2_DEFAULT_CONSUMER) {
        create_default_consumer_sp(parse, spname);
        comdb2prepareNoRows(v, parse, 0, sc, comdb2SqlSchemaChange, (vdbeFuncArgFree)&free_schema_change_type);
    } else {
        comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran, (vdbeFuncArgFree)&free_schema_change_type);
    }
    return;
}

void comdb2DropTrigger(Parse *parse, int consumer, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, consumer ? SQLITE_DROP_LUA_CONSUMER :
                             SQLITE_DROP_LUA_TRIGGER, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    char spname[MAX_SPNAME];

    if (comdb2TokenToStr(proc, spname, sizeof(spname))) {
        sqlite3ErrorMsg(parse, "Procedure name is too long");
        return;
    }

    Q4SP(qname, spname);
    if (!getqueuebyname(qname)) {
        sqlite3ErrorMsg(parse, "no such trigger: %s", spname);
        return;
    }

    // trigger drop table:qname
    struct schema_change_type *sc = new_schemachange_type();
    sc->kind = SC_DEL_TRIGGER;
    strcpy(sc->tablename, qname);
    Vdbe *v = sqlite3GetVdbe(parse);
    comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran, (vdbeFuncArgFree)&free_schema_change_type);
}

#define comdb2CreateFunc(p, proc, pfx, PFX, type, flags)                            \
    do {                                                                            \
        char spname[MAX_SPNAME];                                                    \
        struct schema_change_type *sc = new_schemachange_type();                    \
        sc->kind = SC_ADD_##PFX##FUNC;                                              \
        if (comdb2IsDryrun(p)) {                                                    \
            if (comdb2SCIsDryRunnable(sc)) {                                        \
                (sc)->dryrun = 1;                                                   \
            } else {                                                                \
                sqlite3ErrorMsg(p, "DRYRUN not supported for this operation");      \
                (p)->rc = SQLITE_MISUSE;                                            \
                free_schema_change_type(sc);                                        \
                return;                                                             \
            }                                                                       \
        }                                                                           \
        if (comdb2TokenToStr(proc, spname, sizeof(spname))) {                       \
            sqlite3ErrorMsg(p, "procedure name is too long");                       \
            return;                                                                 \
        }                                                                           \
        if (!comdb2LocateSP(p, spname)) {                                           \
            return;                                                                 \
        }                                                                           \
        if (find_lua_##pfx##func(spname)) {                                         \
            sqlite3ErrorMsg(p, "lua " #type " function:%s already exists", spname); \
            return;                                                                 \
        }                                                                           \
        sc->lua_func_flags |= flags;                                                \
        strcpy(sc->spname, spname);                                                 \
        Vdbe *v = sqlite3GetVdbe(p);                                                \
        comdb2prepareNoRows(v, p, 0, sc, &comdb2SqlSchemaChange_tran,               \
                            (vdbeFuncArgFree)&free_schema_change_type);             \
    } while (0)

void comdb2CreateScalarFunc(Parse *parse, Token *proc, int flags)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_CREATE_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    comdb2CreateFunc(parse, proc, s, S, scalar, flags);
}

void comdb2CreateAggFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_CREATE_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    comdb2CreateFunc(parse, proc, a, A, aggregate, 0);
}

#define comdb2DropFunc(p, proc, pfx, PFX, type)                                     \
    do {                                                                            \
        char spname[MAX_SPNAME];                                                    \
        if (comdb2TokenToStr(proc, spname, sizeof(spname))) {                       \
            sqlite3ErrorMsg(p, "procedure name is too long");                       \
            return;                                                                 \
        }                                                                           \
        if (find_lua_##pfx##func(spname) == 0) {                                    \
            sqlite3ErrorMsg(p, "no such lua " #type " function:%s", spname);        \
            return;                                                                 \
        }                                                                           \
        struct schema_change_type *sc = new_schemachange_type();                    \
        sc->kind = SC_DEL_##PFX##FUNC;                                              \
        if(comdb2IsDryrun(p)){                                                      \
            if(comdb2SCIsDryRunnable(sc)){                                          \
                (sc)->dryrun = 1;                                                   \
            } else {                                                                \
                sqlite3ErrorMsg(p, "DRYRUN not supported for this operation");      \
                (p)->rc = SQLITE_MISUSE;                                            \
                return;                                                             \
            }                                                                       \
        }                                                                           \
        strcpy(sc->spname, spname);                                                 \
        Vdbe *v = sqlite3GetVdbe(p);                                                \
        comdb2prepareNoRows(v, p, 0, sc, &comdb2SqlSchemaChange_tran,               \
                            (vdbeFuncArgFree)&free_schema_change_type);             \
    } while (0)

void comdb2DropScalarFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_DROP_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    char spname[MAX_SPNAME];
    if (comdb2TokenToStr(proc, spname, sizeof(spname))) {
        sqlite3ErrorMsg(parse, "Procedure name is too long");
        return;
    }

    // Even though we know that we can't drop the lua scalar function here
    // , we let this go through and check the error in do_lua_sfunc to have
    // a homogenous rcode
#ifdef SFUNC_USAGE_CHECK_WHEN_PARSE
    char *tbl = 0;
    if (lua_sfunc_used(spname, &tbl)) {
        sqlite3ErrorMsg(parse, "Can't drop. %s is in use by %s", spname, tbl);
        return;
    }
#endif

    comdb2DropFunc(parse, proc, s, S, scalar);
}

void comdb2DropAggFunc(Parse *parse, Token *proc)
{
    if (comdb2IsPrepareOnly(parse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(parse, SQLITE_DROP_LUA_FUNCTION, 0, 0, 0) ){
            sqlite3ErrorMsg(parse, COMDB2_NOT_AUTHORIZED_ERRMSG);
            parse->rc = SQLITE_AUTH;
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(parse))
        return;

    comdb2DropFunc(parse, proc, a, A, aggregate);
}
