#include <notifications_queue.h>
#include <cdb2api.h>
#include <logmsg.h>
#include <cdb2_constants.h>
#include <comdb2.h>
#include <time.h>

extern char gbl_dbname[MAX_DBNAME_LENGTH];
time_t gbl_notifications_queue_last_truncated = 0;

static int create_notifications_queue() {
    cdb2_hndl_tp *db;
    int rc = 0;
    int rc2;

    if (getqueuebyname("__qcomdb2_notifications")) {
        return 0;
    }

    rc = cdb2_open(&db, gbl_dbname, "local", 0);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    char *sql;
    if (get_dbtable_by_name("comdb2_notifications_table") == NULL) {
        sql = "CREATE TABLE comdb2_notifications_table(tablename cstring(100), timestamp datetime, json vutf8(100));";
        rc = cdb2_run_statement(db, sql);
        if (rc != CDB2_OK) {
            logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
            goto done;
        }
    }

    sql = "CREATE PROCEDURE comdb2_notifications VERSION 'v1' {\n"
          "local function define_emit_columns()\n"
          "db:num_columns(3)\n"
          "db:column_name('tablename', 1)\n"
          "db:column_type('cstring', 1)\n"
          "db:column_name('timestamp', 2)\n"
          "db:column_type('datetime', 2)\n"
          "db:column_name('json', 3)\n"
          "db:column_type('vutf8', 3)\n"
          "end\n"
          "local function main()\n"
          "define_emit_columns()\n"
          "-- get handle to consumer associated with stored procedure\n"
          "local consumer = db:consumer()\n"
          "local row\n"
          "local change = consumer:get() -- blocking call\n"
          "if change then\n"
          "row = change.new\n"
          "end\n"
          "if row == nil then\n"
          "row = {}\n"
          "end\n"
          "consumer:emit(row) -- blocking call\n"
          "consumer:consume()\n"
          "end\n"
          "}";
    rc = cdb2_run_statement(db, sql);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

    sql = "CREATE LUA CONSUMER comdb2_notifications ON (TABLE comdb2_notifications_table FOR INSERT)";
    rc = cdb2_run_statement(db, sql);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto done;
    }

done:
    rc2 = cdb2_close(db);
    if (rc2 != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc2, cdb2_errstr(db));
        return rc2;
    }
    return rc;
}

void *add_to_notifications_queue(void *arg) {
    cdb2_hndl_tp *db;
    int rc;
    struct notifications_queue_item *item = (struct notifications_queue_item *)arg;

    if (create_notifications_queue()) {
        goto free_memory;
    }

    rc = cdb2_open(&db, gbl_dbname, "local", 0);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto close;
    }

    char *sql = "insert into comdb2_notifications_table values(@tablename, NOW(), @json);";
    rc = cdb2_bind_param(db, "tablename", CDB2_CSTRING, item->tablename, strlen(item->tablename));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto clear;
    }

    rc = cdb2_bind_param(db, "json", CDB2_CSTRING, item->json, strlen(item->json));
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto clear;
    }

    rc = cdb2_run_statement(db, sql);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
        goto clear;
    }

clear:
    rc = cdb2_clearbindings(db);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
    }

close:
    rc = cdb2_close(db);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "%s: rc = %d, %s\n", __func__, rc, cdb2_errstr(db));
    }

free_memory:
    free(item->tablename);
    free(item->json);
    free(item);

    return NULL;
}
