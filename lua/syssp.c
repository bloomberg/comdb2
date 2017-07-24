#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <sp_int.h>
#include <comdb2.h>
#include <cdb2_constants.h>
#include <analyze.h>
#include <verify.h>
#include <tag.h>
#include <sql.h>

#include <logmsg.h>


/* Wishes for anyone who wants to clean this up one day:
 * 1)  don't need boilerplate lua code for this, should have a fixed description
 *     of types/names for each call, and C code for emitting them. 
 * 2)  real namespaces - _SP.name instead of _SP["name"], load_stored_procedure
 *     creates tables as necessary (necessary?)
 */

/*
 *
 * hostname    text
 * port        int
 * master      int   -- needed?
 * syncmode    int
 * retry       int   -- 10, needed?
 *
 */
static int db_cluster(Lua L)
{
    char *hosts[REPMAX];
    int nnodes;
    struct host_node_info nodes[REPMAX];

    nnodes = net_get_nodes_info(thedb->handle_sibling, REPMAX, nodes);

    lua_createtable(L, nnodes, 0);
    for (int i = 0; i < nnodes; i++) {
        lua_createtable(L, 5, 0);

        lua_pushstring(L, "host");
        lua_pushstring(L, nodes[i].host);
        lua_settable(L, -3);

        lua_pushstring(L, "port");
        lua_pushinteger(L, nodes[i].port);
        lua_settable(L, -3);

        lua_pushstring(L, "master");
        lua_pushinteger(L, nodes[i].host == thedb->master);
        lua_settable(L, -3);

        lua_pushstring(L, "sync");
        lua_pushinteger(L, thedb->rep_sync);
        lua_settable(L, -3);

        lua_pushstring(L, "retry");
        lua_pushinteger(L, thedb->retry);
        lua_settable(L, -3);

        lua_rawseti(L, -2, i+1);
    }

    return 1;
}

/*
 *
 * tablename text
 * dbnum     int
 * lrl       int
 * ixnum     int
 * keysize   int
 * dupes     int
 * recnums   int
 * primary   int
 *
 */
static int db_comdbg_tables(Lua L) {
    struct dbtable *db;
    int rownum = 1;

    /* TODO: locking protocol for this is... */
    lua_createtable(L, 0, 0);
    for (int dbn = 0; dbn < thedb->num_dbs; dbn++) {
        struct dbtable *db;
        db = thedb->dbs[dbn];
        if (db->dbnum) {
            for (int ix = 0; ix < db->nix; ix++) {
                lua_createtable(L, 8, 0); 

                lua_pushstring(L, "tablename");
                lua_pushstring(L, db->dbname);
                lua_settable(L, -3);

                lua_pushstring(L, "dbnum");
                lua_pushinteger(L, db->dbnum);
                lua_settable(L, -3);

                lua_pushstring(L, "lrl");
                lua_pushinteger(L, getdefaultdatsize(db));
                lua_settable(L, -3);

                lua_pushstring(L, "ixnum");
                lua_pushinteger(L, ix);
                lua_settable(L, -3);

                lua_pushstring(L, "keysize");
                lua_pushinteger(L, getdefaultkeysize(db, ix));
                lua_settable(L, -3);

                lua_pushstring(L, "dupes");
                lua_pushinteger(L, db->ix_dupes[ix]);
                lua_settable(L, -3);

                lua_pushstring(L, "recnums");
                lua_pushinteger(L, db->ix_recnums[ix]);
                lua_settable(L, -3);

                lua_pushstring(L, "primary");
                lua_pushinteger(L, 0);
                lua_settable(L, -3);

                lua_rawseti(L, -2, rownum++);
            }
        }
    }

    return 1;
}

/* wrapper to call analyze and process the output */
static int db_comdb_analyze(Lua L) {
    char * tbl = NULL;
    int percent = 0;
    int ovr_percent = 0;
    if (lua_isstring(L, 1)) {
        tbl = (char*) lua_tostring(L, -2);
        if (lua_isnumber(L, 2)) {
            percent = lua_tonumber(L, -1);
            if(percent >= 0 && percent <= 100) ovr_percent = 1;
            else percent = 0;
        }
        else percent = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DEFAULT_ANALYZE_PERCENT);
    }

    lua_settop(L, 0);
    lua_createtable(L, 0, 0);

    FILE *f = tmpfile();
    if (!f) {
        logmsg(LOGMSG_ERROR, "%s:%d SYSTEM RAN OUT OF FILE DESCRIPTORS!!! EXITING\n",
                __FILE__, __LINE__);
        clean_exit();
    }
    SBUF2 *sb = sbuf2open( fileno(f), SBUF2_NO_CLOSE_FD); /* sbuf pointed at f */

    int rownum = 1;

    if(tbl && strlen(tbl) > 0) {
        logmsg(LOGMSG_DEBUG, "db_comdb_analyze: analyze table '%s' at %d percent\n", tbl, percent);
        analyze_table(tbl, sb, percent, ovr_percent);
    }
    else {
        logmsg(LOGMSG_DEBUG, "db_comdb_analyze: analyze database\n");
        analyze_database(sb, 
                         bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DEFAULT_ANALYZE_PERCENT),
                         0);
    }
    sbuf2close(sb);

    rewind(f);
    char buf[1024] = {0};
    char *s;
    while (fgets(buf, sizeof(buf), f)) {
#ifdef DEBUG
        printf("%s\n", buf);
#endif
        s = strchr(buf, '\n');
        if (s) *s = 0;
        if(buf[0] != '>' && buf[0] != '?')  continue; //filter out extra lines

        lua_createtable(L, 0, 1);

        lua_pushstring(L, "out");
        lua_pushstring(L, &buf[1]); //skip first character in buffer
        lua_settable(L, -3);

        lua_rawseti(L, -2, rownum++);
    }
    fclose(f);

    return 1;
}


static int db_comdb_verify(Lua L) {
    SP sp = getsp(L);
    sp->max_num_instructions = 1000000; //allow large number of steps
    char *tbl = NULL;
    if (lua_isstring(L, 1)) {
        tbl = (char *) lua_tostring(L, -1);
    }

    struct column_info col;
    col.type = SQLITE_TEXT;
    strncpy(col.column_name, "out", sizeof(col.column_name));

    CDB2SQLRESPONSE__Column **columns = newsql_alloc_row(1);
    if(!columns)
        return luaL_error(L, "Alloc error.");
    newsql_send_column_info(sp->clnt, &col, 1, NULL, columns);
    newsql_dealloc_row(columns,1);

    int rc = 0;

    if (!tbl || strlen(tbl) < 1) {
        db_verify_table_callback(L, "Usage: verify(\"<table>\")");
        return luaL_error(L, "Verify failed.");
    }

    struct dbtable *t;
    int found = 0;
    for (int dbn = 0; dbn < thedb->num_dbs; dbn++) {
        struct dbtable *t = thedb->dbs[dbn];
        if (strcmp(tbl, t->dbname) == 0) {
            found = 1;
            break;
        }
    }
    if (found) {
        logmsg(LOGMSG_USER, "db_comdb_verify: verify table '%s'\n", tbl);
        rc = verify_table(tbl, NULL, 1, 0, db_verify_table_callback, L); //freq 1, fix 0
    }
    else {
        db_verify_table_callback(L, "Table does not exist.");
        rc = 1;
    }
    if (rc) {
        return luaL_error(L, "Verify failed.");
    }
    else
        db_verify_table_callback(L, "Verify succeeded.");

    return 1;
}


static int db_send(Lua L) {
    FILE *f;
    char buf[1024];
    int rownum = 1;
    char *s;
    char *cmd;

    if (!lua_isstring(L, 1))
        return luaL_error(L, "Expected string argument");

    if (gbl_uses_password) {
      SP sp = getsp(L);
      if (sp && sp->clnt && sp->clnt->user) {
          int bdberr;
          if (bdb_tbl_op_access_get(thedb->bdb_env, NULL, 0, "", sp->clnt->user, &bdberr)) {
              return luaL_error(L, "User doesn't have access to run this command.");
          }
      }
    }

    cmd = (char*) lua_tostring(L, -1);
    lua_settop(L, 0);

    lua_createtable(L, 0, 0);

    f = tmpfile();
    if (!f)
    {
        logmsg(LOGMSG_FATAL, "%s:%d SYSTEM RAN OUT OF FILE DESCRIPTORS!!! EXITING\n",
                __FILE__, __LINE__);
        clean_exit();
    }
    /* kludge spackle, engage */
    io_override_set_std(f);
    process_command(thedb, cmd, strlen(cmd), 0);
    io_override_set_std(NULL);
    rewind(f);
    while (fgets(buf, sizeof(buf), f)) {
        char *s;
        s = strchr(buf, '\n');
        if (s) *s = 0;

        lua_createtable(L, 0, 1);

        lua_pushstring(L, "out");
        lua_pushstring(L, buf);
        lua_settable(L, -3);

        lua_rawseti(L, -2, rownum++);
    }
    fclose(f);

    return 1;
}

static const luaL_Reg sys_funcs[] = {
    { "cluster", db_cluster },
    { "comdbg_tables", db_comdbg_tables },
    { "send", db_send },
    { "comdb_analyze", db_comdb_analyze },
    { "comdb_verify", db_comdb_verify },
    { NULL, NULL }
}; 

struct sp_source {
    char *name;
    char *source;
};

static struct sp_source syssps[] = {
    /* horrible things needed by a proxy to bootstrap */
    {
        "sys.info.cluster",
        "local function main()\n"
        "    local schema = {\n"
        "        { 'string', 'host' },\n"
        "        { 'int',    'port' },\n"
        "        { 'int',    'master'},\n"
        "        { 'int',    'sync'},\n"
        "        { 'int',    'retry'}\n"
        "    }\n"
        "    db:num_columns(table.getn(schema))\n"
        "    for i, v in ipairs(schema) do\n"
        "        db:column_name(v[2], i)\n"
        "        db:column_type(v[1], i)\n"
        "    end\n"
        "    local cluster_info = sys.cluster()\n"
        "    for i, v in ipairs(cluster_info) do\n"
        "        db:emit(v)\n"
        "    end\n"
        "end\n"
    },

    {
        "sys.info.comdbg_tables",
        "local function main()\n"
        "    local schema = {\n"
        "        { 'string', 'tablename' },\n"
        "        { 'int',    'dbnum' },\n"
        "        { 'int',    'lrl'},\n"
        "        { 'int',    'ixnum'},\n"
        "        { 'int',    'keysize'},\n"
        "        { 'int',    'dupes'},\n"
        "        { 'int',    'recnums'},\n"
        "        { 'int',    'primary'}\n"
        "    }\n"
        "    db:num_columns(table.getn(schema))\n"
        "    for i, v in ipairs(schema) do\n"
        "        db:column_name(v[2], i)\n"
        "        db:column_type(v[1], i)\n"
        "    end\n"
        "    local table_info = sys.comdbg_tables()\n"
        "    for i, v in ipairs(table_info) do\n"
        "        db:emit(v)\n"
        "    end\n"
        "end\n"
    },

    {
        "sys.cmd.send",
        "local function main(cmd)\n"
        "    local schema = {\n"
        "        { 'string', 'out' },\n"
        "    }\n"
        "    db:num_columns(table.getn(schema))\n"
        "    for i, v in ipairs(schema) do\n"
        "        db:column_name(v[2], i)\n"
        "        db:column_type(v[1], i)\n"
        "    end\n"
        "    local msg = sys.send(cmd)\n"
        "    for i, v in ipairs(msg) do\n"
        "        db:emit(v)\n"
        "    end\n"
        "end\n"
    },

    {
        // to call analyze for a table: cdb2sql adidb local 'exec procedure sys.cmd.analyze("t1")'
        "sys.cmd.analyze",
        "local function main(tbl, percent)\n"
        "    local schema = {\n"
        "        { 'string', 'out' },\n"
        "    }\n"
        "    db:num_columns(table.getn(schema))\n"
        "    for i, v in ipairs(schema) do\n"
        "        db:column_name(v[2], i)\n"
        "        db:column_type(v[1], i)\n"
        "    end\n"
        "    local msg, rc = sys.comdb_analyze(tbl, percent)\n"
        "    for i, v in ipairs(msg) do\n"
        "        db:emit(v)\n"
        "    end\n"
        "end\n"
    }
    ,{
        // to call verify for a table: cdb2sql adidb local 'exec procedure sys.cmd.verify("t1")'
        "sys.cmd.verify",
        "local function main(tbl)\n"
        "sys.comdb_verify(tbl)\n"
        "end\n"
    }
};

char* find_syssp(const char *s) {
    for (int i = 0; i < sizeof(syssps)/sizeof(syssps[0]); i++) {
        if (strcmp(syssps[i].name, s) == 0)
            return syssps[i].source;
    }
    return NULL;
}

/* We call this while creating new globals is disabled (because we only want to
 * expose these functions for SPs in the _SP.sys namespace). Use raw writes to 
 * avoid triggering the metatable methods that block it. */
void init_sys_funcs(Lua L) {
    lua_getglobal(L, "_G");
    lua_pushstring(L, "sys");
    lua_newtable(L);
    luaL_openlib(L, NULL, sys_funcs, 0);
    lua_rawset(L, -3);
    lua_pop(L, 1);
}
