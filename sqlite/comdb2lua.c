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

int comdb2LocateSP(Parse *p, char *sp)
{
	char *ver = NULL;
	int bdberr;
	int rc0 = bdb_get_sp_get_default_version(sp, &bdberr);
	int rc1 = bdb_get_default_versioned_sp(sp, &ver);
	free(ver);
	if (rc0 < 0 && rc1 < 0) {
		sqlite3ErrorMsg(p, "no such procedure: %s", sp);
		return -1;
	}
	return 0;
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

#define ALLOW_ALL_COLS
static void add_watched_cols(int type, Table *table, Cdb2TrigEvent *event,
			     ColumnEventList *list)
{
	if (event->cols) {
		for (int i = 0; i < event->cols->nId; ++i) {
			ColumnEvent *ce = getcol(list, event->cols->a[i].zName);
			ce->event |= type;
		}
	#ifdef ALLOW_ALL_COLS
	} else {
		for (int i = 0; i < table->nCol; ++i) {
			ColumnEvent *ce = getcol(list, table->aCol[i].zName);
			ce->event |= type;
		}
	#endif
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
	#ifndef ALLOW_ALL_COLS
	} else if (B->cols == NULL) {
		sqlite3DbFree(pParse->db, A);
		sqlite3ErrorMsg(pParse, "%s condition has unspecified columns", type);
		return NULL;
	#endif
	}
	e->op = B->op;
	e->cols = B->cols;
	return A;
}

Cdb2TrigTables *comdb2AddTriggerTable(Parse *parse, Cdb2TrigTables *tables,
				      SrcList *tbl, Cdb2TrigEvents *events)
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

// dynamic -> consumer
void comdb2CreateTrigger(Parse *parse, int dynamic, Token *proc, Cdb2TrigTables *tbl)
{
	TokenStr(spname, proc);
	Q4SP(qname, spname);

	if (getqueuebyname(qname)) {
		sqlite3ErrorMsg(parse, "trigger already exists: %s", spname);
		return;
	}

	if (comdb2LocateSP(parse, spname) != 0) {
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
	sprintf(method, "dest:%s:%s", dynamic ? "dynlua" : "lua", spname);

	// trigger add table:qname dest:method
	struct schema_change_type *sc = new_schemachange_type();
	sc->is_trigger = 1;
	sc->addonly = 1;
	strcpy(sc->table, qname);
	struct dest *d = malloc(sizeof(struct dest));
	d->dest = strdup(method);
	listc_abl(&sc->dests, d);
	sc->newcsc2 = strbuf_disown(s);
	strbuf_free(s);
	Vdbe *v = sqlite3GetVdbe(parse);
	comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran,
			    (vdbeFuncArgFree)&free_schema_change_type);
}

void comdb2DropTrigger(Parse *parse, Token *proc)
{
	TokenStr(spname, proc);
	Q4SP(qname, spname);
	if (!getqueuebyname(qname)) {
		sqlite3ErrorMsg(parse, "no such trigger: %s", spname);
		return;
	}
	// trigger drop table:qname
	struct schema_change_type *sc = new_schemachange_type();
	sc->is_trigger = 1;
	sc->drop_table = 1;
	strcpy(sc->table, qname);
	Vdbe *v = sqlite3GetVdbe(parse);
	comdb2prepareNoRows(v, parse, 0, sc, &comdb2SqlSchemaChange_tran,
			    (vdbeFuncArgFree)&free_schema_change_type);
}

#define comdb2CreateFunc(parse, proc, pfx, type)                               \
	do {                                                                   \
		int bdberr = 0;                                                \
		TokenStr(spname, proc);                                        \
		if (comdb2LocateSP(parse, spname) != 0) {                      \
			return;                                                \
		}                                                              \
		if (find_lua_##pfx##func(spname)) {                            \
			sqlite3ErrorMsg(parse,                                 \
					"lua " #type "func:%s already exists", \
					spname);                               \
			return;                                                \
		}                                                              \
		struct schema_change_type *sc = new_schemachange_type();       \
		sc->is_##pfx##func = 1;                                        \
		sc->addonly = 1;                                               \
		strcpy(sc->spname, spname);                                    \
		Vdbe *v = sqlite3GetVdbe(parse);                               \
		comdb2prepareNoRows(                                           \
		    v, parse, 0, sc, &comdb2SqlSchemaChange_tran,              \
		    (vdbeFuncArgFree)&free_schema_change_type);                \
	} while (0)

void comdb2CreateScalarFunc(Parse *parse, Token *proc)
{
	comdb2CreateFunc(parse, proc, s, scalar);
}

void comdb2CreateAggFunc(Parse *parse, Token *proc)
{
	comdb2CreateFunc(parse, proc, a, aggregate);
}

#define comdb2DropFunc(parse, proc, pfx, type)                                 \
	do {                                                                   \
		int bdberr = 0;                                                \
		TokenStr(spname, proc);                                        \
		if (find_lua_##pfx##func(spname) == 0) {                       \
			sqlite3ErrorMsg(parse, "no such lua " #type "func:%s", \
					spname);                               \
			return;                                                \
		}                                                              \
		struct schema_change_type *sc = new_schemachange_type();       \
		sc->is_##pfx##func = 1;                                        \
		sc->addonly = 0;                                               \
		strcpy(sc->spname, spname);                                    \
		Vdbe *v = sqlite3GetVdbe(parse);                               \
		comdb2prepareNoRows(                                           \
		    v, parse, 0, sc, &comdb2SqlSchemaChange_tran,              \
		    (vdbeFuncArgFree)&free_schema_change_type);                \
	} while (0)

void comdb2DropScalarFunc(Parse *parse, Token *proc)
{
	comdb2DropFunc(parse, proc, s, scalar);
}

void comdb2DropAggFunc(Parse *parse, Token *proc)
{
	comdb2DropFunc(parse, proc, a, aggregate);
}

