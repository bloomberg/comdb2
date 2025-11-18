/**
 * Support for writeable views using triggers
 *
 *
 * Dr. Hipp's example :

CREATE TABLE A(a INTEGER PRIMARY KEY,b,c,d);
CREATE TABLE B(a INTEGER PRIMARY KEY,b,c,d);
WITH RECURSIVE
  c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<9)
INSERT INTO A(a,b,c,d) SELECT x, x+100, x+1000, x+10000 FROM c;
WITH RECURSIVE
  c(x) AS (VALUES(10) UNION ALL SELECT x+1 FROM c WHERE x<19)
INSERT INTO B(a,b,c,d) SELECT x, x+100, x+1000, x+10000 FROM c;

CREATE VIEW V AS
  SELECT * FROM A WHERE a<10
  UNION ALL
  SELECT * FROM B WHERE a>=10;

CREATE TRIGGER r1 INSTEAD OF UPDATE ON V BEGIN
  UPDATE A SET b=new.b, c=new.c, d=new.d WHERE a=old.a;
  UPDATE B SET b=new.b, c=new.c, d=new.d WHERE a=old.a;
END;

.header on
.mode column
.print *** BEFORE THE UPDATE
SELECT * FROM V;
.print *** AFTER UPDATE b=12345 WHERE a=11
UPDATE V SET b=12345 WHERE a=11;
SELECT * FROM V;
.print *** SELECT A
SELECT * FROM A;
.print *** SELECT B
SELECT * FROM B;

 *
 */

/**
 * Support for the writes for time-based partitioned tables
 *
 * Deletes:
 * - trigger will delete from all the tables of the view
 * Updates:
 * - trigger will update all the tables using the same condition
 * Inserts:
 * - trigger will be updated when the tables are rollout and will
 * insert into the latest shard
 * Actually the rollout is a multistep procedure:
 * - create new shard;  the views on proper
 * - switch views on specified time;
 * - delete oldest shard, if needed
 * It is on the 2nd action, the actual views switch, at which point
 * the insert will move to the lastest shard.
 *
 *
 */

#define TRIGGER_SUFFIX_UPD "upd"
#define TRIGGER_SUFFIX_INS "ins"
#define TRIGGER_SUFFIX_DEL "del"

char *_views_create_delete_trigger_query(timepart_view_t *view,
                                         struct errstat *err)
{
    char *ret_str = NULL;
    char *tmp_str = NULL;

    int i;

    ret_str =
        sqlite3_mprintf("CREATE TRIGGER \"%w_%w\" INSTEAD OF DELETE ON \"%w\" BEGIN",
                        view->name, TRIGGER_SUFFIX_DEL, view->name);
    if (!ret_str) {
        goto oom;
    }
    for (i = 0; i < view->nshards; i++) {
        tmp_str = sqlite3_mprintf(
            "%s\nDELETE FROM \"%w\" where rowid=old.__hidden__rowid;", ret_str,
            view->shards[i].tblname);
        sqlite3_free(ret_str);
        ret_str = tmp_str;
    }
    tmp_str = sqlite3_mprintf("%s\nEND;", ret_str);
    sqlite3_free(ret_str);
    ret_str = tmp_str;

    errstat_set_rc(err, VIEW_NOERR);

    dbg_verbose_sqlite("Generated delete trigger:\n%s\n", ret_str);
    return ret_str;

oom:
    errstat_set_rc(err, VIEW_ERR_MALLOC);
    errstat_set_strf(err, "%s Malloc OOM", __func__);

    return NULL;
}

static char *_views_destroy_trigger_query(const char *view_name,
                                          struct errstat *err,
                                          const char *suffix, const char *dbgname)
{
    char *ret_str = NULL;

    ret_str = sqlite3_mprintf("DROP TRIGGER \"%w_%w\"", view_name, suffix);
    if (!ret_str) {
        goto oom;
    }

    errstat_set_rc(err, VIEW_NOERR);

    dbg_verbose_sqlite("Drop %s trigger:\n%s\n", dbgname, ret_str);
    return ret_str;

oom:
    if (ret_str)
        free(ret_str);
    errstat_set_rc(err, VIEW_ERR_MALLOC);
    errstat_set_strf(err, "%s Malloc OOM", __func__);

    return NULL;
}

char *_views_destroy_delete_trigger_query(const char *view_name,
                                          struct errstat *err)
{
    return _views_destroy_trigger_query(view_name, err, TRIGGER_SUFFIX_DEL, "delete");
}

char *_views_create_update_trigger_query(timepart_view_t *view,
                                         struct errstat *err)
{
    char *ret_str = NULL;
    char *tmp_str = NULL;
    char *cols_str = NULL;

    int i;

    ret_str =
        sqlite3_mprintf("CREATE TRIGGER \"%w_%w\" INSTEAD OF UPDATE ON \"%w\" BEGIN",
                        view->name, TRIGGER_SUFFIX_UPD, view->name);
    if (!ret_str) {
        goto oom;
    }

    assert(view->nshards >= 1);

    cols_str = describe_row(view->shards[view->current_shard].tblname, NULL,
                             VIEWS_TRIGGER_UPDATE, err);
    if (!cols_str) {
        sqlite3_free(ret_str);
        return NULL;
    }

    for (i = 0; i < view->nshards; i++) {
        tmp_str = sqlite3_mprintf(
            "%s\nUPDATE \"%w\" SET %s where rowid=old.__hidden__rowid;", ret_str,
            view->shards[i].tblname, cols_str);
        sqlite3_free(ret_str);
        ret_str = tmp_str;
    }
    tmp_str = sqlite3_mprintf("%s\nEND;", ret_str);
    sqlite3_free(ret_str);
    ret_str = tmp_str;

    errstat_set_rc(err, VIEW_NOERR);

    dbg_verbose_sqlite("Generated update trigger:\n%s\n", ret_str);
    return ret_str;

oom:
    if (ret_str)
        free(ret_str);
    errstat_set_rc(err, VIEW_ERR_MALLOC);
    errstat_set_strf(err, "%s Malloc OOM", __func__);

    return NULL;
}

char *_views_destroy_update_trigger_query(const char *view_name,
                                          struct errstat *err)
{
    return _views_destroy_trigger_query(view_name, err, TRIGGER_SUFFIX_UPD, "update");
}

char *_views_create_insert_trigger_query(timepart_view_t *view,
                                         struct errstat *err)
{
    char *ret_str = NULL;
    char *cols_str = NULL;

    assert(view->nshards >= 1);

    cols_str = describe_row(view->shards[view->current_shard].tblname, NULL,
                             VIEWS_TRIGGER_INSERT, err);
    if (!cols_str) {
        goto oom;
    }

    ret_str = sqlite3_mprintf(
        "CREATE TRIGGER \"%w_%w\" INSTEAD OF INSERT ON \"%w\" BEGIN\n"
        "INSERT INTO \"%w\" VALUES ( %s );\n"
        "END;\n",
        view->name, TRIGGER_SUFFIX_INS, view->name,
        view->shards[view->current_shard].tblname, cols_str);
    if (!ret_str) {
        goto oom;
    }

    sqlite3_free(cols_str);

    errstat_set_rc(err, VIEW_NOERR);

    dbg_verbose_sqlite("Generated insert trigger:\n%s\n", ret_str);
    return ret_str;

oom:
    if (cols_str)
        sqlite3_free(cols_str);
    errstat_set_rc(err, VIEW_ERR_MALLOC);
    errstat_set_strf(err, "%s Malloc OOM", __func__);

    return NULL;
}

char *_views_destroy_insert_trigger_query(const char *view_name,
                                          struct errstat *err)
{
    return _views_destroy_trigger_query(view_name, err, "ins", "insert");
}
