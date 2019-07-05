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
#include <parse_lsn.h>
#include <truncate_log.h>
#include <bdb_api.h>
#include <phys_rep.h>


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
 * uniqnulls int
 *
 */
static int db_comdbg_tables(Lua L) {
    int rownum = 1;

    /* TODO: locking protocol for this is... */
    lua_createtable(L, 0, 0);
    for (int dbn = 0; dbn < thedb->num_dbs; dbn++) {
        struct dbtable *db = thedb->dbs[dbn];
        if (db->dbnum) {
            for (int ix = 0; ix < db->nix; ix++) {
                lua_createtable(L, 8, 0); 

                lua_pushstring(L, "tablename");
                lua_pushstring(L, db->tablename);
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

                lua_pushstring(L, "uniqnulls");
                lua_pushinteger(L, db->ix_nullsallowed[ix]);
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
    char *tblname = NULL;
    if (lua_isstring(L, 1)) {
        tblname = (char *) lua_tostring(L, -1);
    }

    char *cols[] = {"out"};
    struct sqlclntstate *clnt = sp->clnt;
    write_response(clnt, RESPONSE_COLUMNS_STR, &cols, 1);

    int rc = 0;

    if (!tblname || strlen(tblname) < 1) {
        db_verify_table_callback(L, "Usage: verify(\"<table>\")");
        return luaL_error(L, "Verify failed.");
    }

    rdlock_schema_lk();
    struct dbtable *db = get_dbtable_by_name(tblname);
    unlock_schema_lk();
    if (db) {
        logmsg(LOGMSG_USER, "db_comdb_verify: verify table '%s'\n", tblname);
        rc = verify_table(tblname, NULL, 1, 0, db_verify_table_callback, L); //freq 1, fix 0
    }
    else {
        char buf[128] = {0};
        snprintf(buf, sizeof(buf), "Table \"%s\" does not exist.", tblname);
        db_verify_table_callback(L, buf);
        rc = 1;
    }
    if (rc) {
        return luaL_error(L, "Verify failed.");
    }
    else
        db_verify_table_callback(L, "Verify succeeded.");

    return 1;
}

int gbl_truncating_log = 0;

static int db_comdb_truncate_log(Lua L) {
    SP sp = getsp(L);
    sp->max_num_instructions = 1000000; //allow large number of steps
    char *lsnstr = NULL;
    if (lua_isstring(L, 1)) {
        lsnstr = (char *) lua_tostring(L, 1);
    }
    else {
        return luaL_error(L, "Require a string for the lsn");
    }

    int rc;
    unsigned int file, offset;
  
    if ((rc = char_to_lsn(lsnstr, &file, &offset)) != 0) {
        // db_verify_table_callback(L, "Usage: truncate_log(\"{<file>:<offset>}\")");
        return luaL_error(L, 
                "Usage: truncate_log(\"{<file>:<offset>}\"). Input not valid.");
    }

    logdelete_lock(__func__, __LINE__);
    gbl_truncating_log = 1;
    logdelete_unlock(__func__, __LINE__);

#if 0
    int min_file, min_offset;
    bdb_min_truncate(thedb->bdb_env, &min_file, &min_offset, NULL);

    if (min_file == 0) {
        gbl_truncating_log = 0;
        return luaL_error(L, "Log is not truncatable");
    }

    if (file < min_file || (file == min_file && 
                offset < min_offset)) {
        return luaL_error(L, 
                "Minimum truncate lsn is {%d:%d}", min_file, min_offset);
    }
#endif
    logmsg(LOGMSG_INFO, "applying log from lsn {%u:%u}\n", file, offset);

    rc = truncate_log(file, offset, 1);
    gbl_truncating_log = 0;
    if (rc != 0)
    {
        if (rc == -1)
            return luaL_error(L, "Can only truncate from master node");
        else
            return luaL_error(L, "Couldn't truncate to lsn {%u:%u}", file, offset);
    }

    return 1;
}

static int db_comdb_truncate_time(Lua L) {
    SP sp = getsp(L);
    sp->max_num_instructions = 1000000; //allow large number of steps
    time_t time;
    int rc;
    if (lua_isnumber(L, 1))
    {
        time = (time_t) lua_tointeger(L, 1);
    }
    else
    {
        return luaL_error(L, "Usage: truncate_time(<time>), "
                "where time is epoch time");
    }

    logdelete_lock(__func__, __LINE__);
    gbl_truncating_log = 1;
    logdelete_unlock(__func__, __LINE__);

#if 0
    int32_t min_time;
    bdb_min_truncate(thedb->bdb_env, NULL, NULL, &min_time);

    if (time < min_time)
    {
        gbl_truncating_log = 0;
        return luaL_error(L, "Minimum truncate timestamp is %d\n",
                min_time);
    }

#endif
    logmsg(LOGMSG_INFO, "Finding earliest log before stated time: %ld.\n", time);

    rc = truncate_timestamp(time);
    gbl_truncating_log = 0;

    if (rc != 0)
    {
        if (rc == -1)
            return luaL_error(L, "Can only truncate from master node");
        else
            return luaL_error(L, "Couldn't truncate to timestamp %ld", time);
    }

    return 1;
}


static int db_comdb_apply_log(Lua L) {
    SP sp = getsp(L);
    sp->max_num_instructions = 1000000; //allow large number of steps
    char *lsnstr = NULL;
    int rc, newfile;
    blob_t blob; 

    if (lua_isstring(L, 1)) {
        lsnstr = (char *) lua_tostring(L, 1);
    }
    else {
        return luaL_error(L, 
                "Usage: apply_log(\"{<file>:<offset>}\", 'blob'). "
                "1st param not string.");
    }

    luabb_toblob(L, 2, &blob);

    if (lua_isnumber(L, 3))
    {
        newfile = (int) lua_tointeger(L, 1);
    }
    else {
        return luaL_error(L, 
                "Usage: apply_log(\"{<file>:<offset>}\", 'blob'). "
                "3rd param not int flag.");
    }

    unsigned int file, offset; 
  
    if ((rc = char_to_lsn(lsnstr, &file, &offset)) != 0) {
        return luaL_error(L, 
                "Usage: apply_log(\"{<file>:<offset>}\", 'blob'). "
                "LSN not valid.");
    }
    logmsg(LOGMSG_USER, "applying log lsn {%u:%u}\n", file, offset);

    if ((rc = apply_log_procedure(file, offset, blob.data, 
                    blob.length, newfile)) != 0)
    {
        return luaL_error(L, "Log apply failed.");
    }

    return 1;
}


static int db_send(Lua L) {
    FILE *f;
    char buf[1024];
    int rownum = 1;
    char *cmd;
    SP sp = getsp(L);

    if (sp) {
        sp->max_num_instructions = 1000000; //allow large number of steps
    }

    if (!lua_isstring(L, 1))
        return luaL_error(L, "Expected string argument");

    if (gbl_uses_password) {
      if (sp && sp->clnt) {
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

static int db_comdb_start_replication(Lua L)
{
    int rc;

    if (!gbl_is_physical_replicant)
    {
        return luaL_error(L, "Database is not a physical replicant, cannot replicate");
    }

    if ((rc = start_replication()) != 0)
    {
        if (rc > 0)
        {
            return luaL_error(L, "DB is already replicating");
        }
        return luaL_error(L, "Couldn't start replicating");
    }

    return 1;
}

static int db_comdb_stop_replication(Lua L)
{
    if (!gbl_is_physical_replicant)
    {
        return luaL_error(L, "Database is not a physical replicant, cannot replicate");
    }

    if (stop_replication() != 0)
    {
        return luaL_error(L, "Something went horribly wrong. Replicating thread wouldn't stop");
    }

    return 1;
}

extern char gbl_dbname[MAX_DBNAME_LENGTH];

static int db_comdb_register_replicant(Lua L)
{
    int nnodes;
    struct host_node_info nodes[REPMAX];
    nnodes = net_get_nodes_info(thedb->handle_sibling, REPMAX, nodes);

    lua_createtable(L, nnodes, 0);
    for (int i = 0; i < nnodes; i++) {
        lua_createtable(L, 3, 0);

        lua_pushstring(L, "tier");
        lua_pushinteger(L, 0);
        lua_settable(L, -3);

        lua_pushstring(L, "dbname");
        lua_pushstring(L, gbl_dbname);
        lua_settable(L, -3);

        lua_pushstring(L, "host");
        lua_pushstring(L, nodes[i].host);
        lua_settable(L, -3);

        lua_rawseti(L, -2, i+1);
    }

    return 1;
}

static const luaL_Reg sys_funcs[] = {
    { "cluster", db_cluster },
    { "comdbg_tables", db_comdbg_tables },
    { "send", db_send },
    { "load", db_csvcopy},
    { "comdb_analyze", db_comdb_analyze },
    { "comdb_verify", db_comdb_verify },
    { "truncate_log", db_comdb_truncate_log },
    { "truncate_time", db_comdb_truncate_time },
    { "apply_log", db_comdb_apply_log },
    { "start_replication", db_comdb_start_replication },
    { "stop_replication", db_comdb_stop_replication },
    { "register_replicant", db_comdb_register_replicant },
    { NULL, NULL }
}; 

struct sp_source {
    char *name;
    char *source;
    char *override;
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
        "end\n",
        NULL
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
        "end\n",
        NULL
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
        "end\n",
        NULL
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
        "end\n",
        NULL
    }
    ,{
        // to call verify for a table: cdb2sql adidb local 'exec procedure sys.cmd.verify("t1")'
        "sys.cmd.verify",
        "local function main(tbl)\n"
        "sys.comdb_verify(tbl)\n"
        "end\n",
        NULL
    }
    ,{
        "sys.cmd.load",
        "local function main(csv,tbl,seperator, header)\n"
        "sys.load(db,csv,tbl,seperator,header)\n"
        "end\n",
        NULL
    }
    ,{
        "sys.cmd.truncate_log",
        "local function main(lsn)\n"
        "sys.truncate_log(lsn)\n"
        "end\n",
        NULL
    }
    ,{
        "sys.cmd.truncate_time",
        "local function main(time)\n"
        "sys.truncate_time(time)\n"
        "end\n",
        NULL
    }
    ,{
        "sys.cmd.apply_log",
        "local function main(lsn, blob, newfile)\n"
        "sys.apply_log(lsn, blob, newfile)\n"
        "end\n",
        NULL
    }
    /* starts and stop replication*/
    ,{
        "sys.cmd.start_replication",
        "local function main()\n"
        "sys.start_replication()\n"
        "end\n",
        NULL
    }
    ,{
        "sys.cmd.stop_replication",
        "local function main()\n"
        "sys.stop_replication()\n"
        "end\n",
        NULL
    }

    /* allow replication assignment */
    ,{
        "sys.cmd.register_replicant",
        "local function main(dbname, machname, file, offset)\n"
        "    local schema = {\n"
        "        { 'int',    'tier' },\n"
        "        { 'string', 'dbname' },\n"
        "        { 'string', 'host' },\n"
        "    }\n"
        "    db:num_columns(table.getn(schema))\n"
        "    for i, v in ipairs(schema) do\n"
        "        db:column_name(v[2], i)\n"
        "        db:column_type(v[1], i)\n"
        "    end\n"
        "    local rep_machs\n"
        "    rep_machs = sys.register_replicant(dbname, machname, file, offset)\n"
        "    for i, v in ipairs(rep_machs) do\n"
        "        db:emit(v)\n"
        "    end\n"
        "end\n",
        "register_replicant"
    }
};

char* find_syssp(const char *s, char **override) {
    for (int i = 0; i < sizeof(syssps)/sizeof(syssps[0]); i++) {
        if (strcmp(syssps[i].name, s) == 0) {
            (*override) = syssps[i].override;
            return syssps[i].source;
        }
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
